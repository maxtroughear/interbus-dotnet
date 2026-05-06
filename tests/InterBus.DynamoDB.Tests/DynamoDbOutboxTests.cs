using System.Collections.Concurrent;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.DynamoDB.Extensions;
using InterBus.DynamoDB.Outbox;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.DynamoDB.Tests;

// ---------------------------------------------------------------------------
// Outbox-specific message types
// ---------------------------------------------------------------------------

public record OutboxOrderPlaced(Guid OrderId, string CustomerEmail);

public record OutboxOrderShipped(Guid OrderId);

// ---------------------------------------------------------------------------
// Trackers
// ---------------------------------------------------------------------------

public sealed class OutboxOrderPlacedTracker
{
    private readonly ConcurrentBag<OutboxOrderPlaced> _messages = [];
    public IReadOnlyCollection<OutboxOrderPlaced> Messages => _messages;
    public void Track(OutboxOrderPlaced msg) => _messages.Add(msg);
}

public sealed class OutboxOrderShippedTracker
{
    private readonly ConcurrentBag<OutboxOrderShipped> _messages = [];
    public IReadOnlyCollection<OutboxOrderShipped> Messages => _messages;
    public void Track(OutboxOrderShipped msg) => _messages.Add(msg);
}

// ---------------------------------------------------------------------------
// Consumers
// ---------------------------------------------------------------------------

public sealed class OutboxOrderPlacedConsumer(OutboxOrderPlacedTracker tracker)
    : IConsumer<OutboxOrderPlaced>
{
    public Task Consume(OutboxOrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Consumer that publishes <see cref="OutboxOrderShipped"/> to the outbox when it receives
/// <see cref="OutboxOrderPlaced"/>. Calls <see cref="IDynamoDbOutboxContext.CommitAsync"/>
/// so the outbox row is atomically committed.
/// </summary>
public sealed class OutboxOrderPlacedPublishingConsumer(
    IDynamoDbOutboxContext outboxContext,
    IPublisher publisher)
    : IConsumer<OutboxOrderPlaced>
{
    public async Task Consume(OutboxOrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        await publisher.PublishAsync(new OutboxOrderShipped(message.OrderId), cancellationToken);
        await outboxContext.CommitAsync(cancellationToken);
    }
}

public sealed class OutboxOrderShippedConsumer(OutboxOrderShippedTracker tracker)
    : IConsumer<OutboxOrderShipped>
{
    public Task Consume(OutboxOrderShipped message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

[TestFixture]
public class DynamoDbOutboxTests
{
    private readonly LocalStackContainer? _container = TestHelpers.CreateLocalStackContainer();
    private IAmazonDynamoDB _dynamoDb = null!;
    private string _tableName = string.Empty;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        if (_container is null)
            return;
        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_container is null)
            return;
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        var runId = Guid.NewGuid().ToString("N");
        _tableName = $"{runId}-InterBusOutboxTest";

        var awsOptions = TestHelpers.CreateAwsOptions(_container);
        var services = new ServiceCollection();
        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);
        services.AddAWSService<IAmazonDynamoDB>();

        var provider = services.BuildServiceProvider();
        _dynamoDb = provider.GetRequiredService<IAmazonDynamoDB>();

        await EnsureOutboxTableExistsAsync(_dynamoDb, _tableName);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _dynamoDb.DeleteTableAsync(_tableName);
        _dynamoDb.Dispose();
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// <summary>
    /// Publishing a message and calling CommitAsync stores the message in the outbox table
    /// before the processor runs.
    /// </summary>
    [Test]
    public async Task Publish_CommitAsync_StoresMessageInOutboxTable()
    {
        var tracker = new OutboxOrderPlacedTracker();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxOrderPlacedConsumer>();
            // Long poll interval so the processor does not run during assertions.
            bus.UseDynamoDbOutbox(o => o
                .WithTableName(_tableName)
                .WithPollInterval(TimeSpan.FromHours(1))
                .DisableOutboxProcessor());
        }, services => services.AddSingleton(tracker));

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var outboxContext = scope.ServiceProvider.GetRequiredService<IDynamoDbOutboxContext>();

            var msg = new OutboxOrderPlaced(Guid.NewGuid(), "test@example.com");
            await publisher.PublishAsync(msg, CancellationToken.None);

            // Message should NOT have been forwarded yet — outbox middleware buffered it.
            Assert.That(tracker.Messages, Is.Empty);

            // CommitAsync atomically writes the outbox group + message to DynamoDB.
            await outboxContext.CommitAsync(CancellationToken.None);

            // Consumer still should not have received it (processor hasn't run).
            Assert.That(tracker.Messages, Is.Empty);

            // Verify the outbox group header and message are in the table.
            var scanResponse = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
            }, CancellationToken.None);

            // Expect 2 items: 1 group header + 1 message item.
            Assert.That(scanResponse.Count, Is.EqualTo(2));

            var groupItem = scanResponse.Items
                .Single(i => i[DynamoDbOutboxTable.EntryId].S == DynamoDbOutboxTable.GroupEntryValue);
            var msgItem = scanResponse.Items
                .Single(i => i[DynamoDbOutboxTable.EntryId].S != DynamoDbOutboxTable.GroupEntryValue);

            Assert.That(groupItem[DynamoDbOutboxTable.GroupId].S,
                Is.EqualTo(msgItem[DynamoDbOutboxTable.GroupId].S));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    /// <summary>
    /// The outbox processor must forward stored messages to the transport and delete them.
    /// </summary>
    [Test]
    public async Task OutboxProcessor_ForwardsMessages_ToTransport()
    {
        var tracker = new OutboxOrderPlacedTracker();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxOrderPlacedConsumer>();
            bus.UseDynamoDbOutbox(o => o
                .WithTableName(_tableName)
                .WithPollInterval(TimeSpan.FromMilliseconds(50)));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var outboxContext = scope.ServiceProvider.GetRequiredService<IDynamoDbOutboxContext>();

                var msg = new OutboxOrderPlaced(Guid.NewGuid(), "deliver@example.com");
                await publisher.PublishAsync(msg, CancellationToken.None);
                await outboxContext.CommitAsync(CancellationToken.None);
            }

            await StartHostedServicesAsync(provider, cts.Token);

            await TestHelpers.WaitUntilAsync(() => tracker.Messages.Count == 1);

            Assert.That(tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(tracker.Messages.Single().CustomerEmail, Is.EqualTo("deliver@example.com"));

            // Processor should have deleted the outbox items after forwarding.
            var scanResponse = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
            }, CancellationToken.None);
            Assert.That(scanResponse.Count, Is.EqualTo(0));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    /// <summary>
    /// Multiple messages committed in the same scope share the same GroupId and are all forwarded.
    /// </summary>
    [Test]
    public async Task OutboxProcessor_ForwardsMultipleMessages_FromSameGroup()
    {
        var tracker = new OutboxOrderPlacedTracker();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxOrderPlacedConsumer>();
            bus.UseDynamoDbOutbox(o => o
                .WithTableName(_tableName)
                .WithPollInterval(TimeSpan.FromMilliseconds(50)));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var outboxContext = scope.ServiceProvider.GetRequiredService<IDynamoDbOutboxContext>();

                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "a@example.com"),
                    CancellationToken.None);
                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "b@example.com"),
                    CancellationToken.None);

                await outboxContext.CommitAsync(CancellationToken.None);
            }

            await StartHostedServicesAsync(provider, cts.Token);

            await TestHelpers.WaitUntilAsync(() => tracker.Messages.Count == 2);

            Assert.That(tracker.Messages, Has.Count.EqualTo(2));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    /// <summary>
    /// A consumer can use the outbox to publish messages. The outbox processor
    /// then forwards those messages to the transport.
    /// </summary>
    [Test]
    public async Task Consumer_PublishViaOutbox_MessageForwardedByProcessor()
    {
        var shippedTracker = new OutboxOrderShippedTracker();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxOrderPlacedPublishingConsumer>(c => c.UseDynamoDbOutbox());
            bus.AddConsumer<OutboxOrderShippedConsumer>();
            bus.AddDynamoDbOutbox(o => o
                .WithTableName(_tableName)
                .WithPollInterval(TimeSpan.FromMilliseconds(50)));
        }, services => services.AddSingleton(shippedTracker));

        try
        {
            // Start hosted services BEFORE publishing so the InMemory transport topology
            // is built and the consumer can receive the OutboxOrderPlaced message.
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "consumer@example.com"),
                    CancellationToken.None);
            }

            await TestHelpers.WaitUntilAsync(() => shippedTracker.Messages.Count == 1);

            Assert.That(shippedTracker.Messages, Has.Count.EqualTo(1));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    /// <summary>
    /// AddTransactItem items are committed atomically with outbox messages.
    /// </summary>
    [Test]
    public async Task CommitAsync_AtomicallyWritesBusinessItemsAndOutboxMessages()
    {
        var tracker = new OutboxOrderPlacedTracker();
        var businessTable = $"{Guid.NewGuid():N}-BusinessTable";

        await EnsureSimpleTableExistsAsync(_dynamoDb, businessTable);

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxOrderPlacedConsumer>();
            bus.UseDynamoDbOutbox(o => o
                .WithTableName(_tableName)
                .WithPollInterval(TimeSpan.FromHours(1))
                .DisableOutboxProcessor());
        }, services => services.AddSingleton(tracker));

        try
        {
            var businessId = Guid.NewGuid().ToString("N");

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var outboxContext = scope.ServiceProvider.GetRequiredService<IDynamoDbOutboxContext>();

                // Register a business DynamoDB write.
                outboxContext.AddTransactItem(new Amazon.DynamoDBv2.Model.TransactWriteItem
                {
                    Put = new Amazon.DynamoDBv2.Model.Put
                    {
                        TableName = businessTable,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            ["Id"] = new() { S = businessId },
                            ["Data"] = new() { S = "test-value" },
                        },
                    },
                });

                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "atomic@example.com"),
                    CancellationToken.None);

                // Single atomic commit writes both the business item and the outbox message.
                await outboxContext.CommitAsync(CancellationToken.None);
            }

            // Verify business item was written.
            var getResponse = await _dynamoDb.GetItemAsync(new GetItemRequest
            {
                TableName = businessTable,
                Key = new Dictionary<string, AttributeValue>
                {
                    ["Id"] = new() { S = businessId },
                },
            }, CancellationToken.None);
            Assert.That(getResponse.IsItemSet, Is.True);

            // Verify outbox message was also written.
            var scanResponse = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
            }, CancellationToken.None);
            Assert.That(scanResponse.Count, Is.EqualTo(2)); // group header + message
        }
        finally
        {
            await StopBusAsync(provider, cts);
            try
            {
                await _dynamoDb.DeleteTableAsync(businessTable);
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }

    /// <summary>
    /// When inline delivery is configured and succeeds, the group is deleted immediately.
    /// </summary>
    [Test]
    public async Task Consumer_UseOutboxWithInlineDelivery_MessageDeliveredImmediately()
    {
        var shippedTracker = new OutboxOrderShippedTracker();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxOrderPlacedPublishingConsumer>(
                c => c.UseDynamoDbOutboxWithInlineDelivery());
            bus.AddConsumer<OutboxOrderShippedConsumer>();
            bus.AddDynamoDbOutbox(o => o
                .WithTableName(_tableName)
                .WithPollInterval(TimeSpan.FromHours(1))
                .DisableOutboxProcessor());
        }, services => services.AddSingleton(shippedTracker));

        try
        {
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "inline@example.com"),
                    CancellationToken.None);
            }

            await TestHelpers.WaitUntilAsync(() => shippedTracker.Messages.Count == 1);

            Assert.That(shippedTracker.Messages, Has.Count.EqualTo(1));

            // After successful inline delivery, the outbox table should be empty.
            await TestHelpers.WaitUntilAsync(
                () => _dynamoDb.ScanAsync(new ScanRequest { TableName = _tableName })
                    .GetAwaiter().GetResult().Count == 0,
                timeoutMs: 5000);

            var scanResponse = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
            }, CancellationToken.None);
            Assert.That(scanResponse.Count, Is.EqualTo(0));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartBusAsync(
        Action<IInterBusBuilder> configureBus,
        Action<IServiceCollection>? extraServices = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var awsOptions = TestHelpers.CreateAwsOptions(_container);
        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);

        services.AddAWSService<IAmazonDynamoDB>();
        extraServices?.Invoke(services);
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        return (provider, cts);
    }

    private static async Task StartHostedServicesAsync(ServiceProvider provider, CancellationToken ct)
    {
        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(ct);
    }

    private static async Task StopBusAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try
            {
                await svc.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
            }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    internal static async Task EnsureOutboxTableExistsAsync(IAmazonDynamoDB dynamoDb, string tableName)
    {
        try
        {
            await dynamoDb.DescribeTableAsync(tableName);
        }
        catch (ResourceNotFoundException)
        {
            var gsiName = new DynamoDbOutboxOptions().GsiIndexName!;

            await dynamoDb.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                KeySchema =
                [
                    new KeySchemaElement(DynamoDbOutboxTable.GroupId, KeyType.HASH),
                    new KeySchemaElement(DynamoDbOutboxTable.EntryId, KeyType.RANGE),
                ],
                AttributeDefinitions =
                [
                    new AttributeDefinition(DynamoDbOutboxTable.GroupId, ScalarAttributeType.S),
                    new AttributeDefinition(DynamoDbOutboxTable.EntryId, ScalarAttributeType.S),
                    new AttributeDefinition(DynamoDbOutboxTable.Shard, ScalarAttributeType.N),
                    new AttributeDefinition(DynamoDbOutboxTable.VisibleAt, ScalarAttributeType.N),
                ],
                GlobalSecondaryIndexes =
                [
                    new GlobalSecondaryIndex
                    {
                        IndexName = gsiName,
                        KeySchema =
                        [
                            new KeySchemaElement(DynamoDbOutboxTable.Shard, KeyType.HASH),
                            new KeySchemaElement(DynamoDbOutboxTable.VisibleAt, KeyType.RANGE),
                        ],
                        Projection = new Projection { ProjectionType = ProjectionType.ALL },
                    },
                ],
                BillingMode = BillingMode.PAY_PER_REQUEST,
            });

            await WaitForTableActiveAsync(dynamoDb, tableName);
        }
    }

    private static async Task EnsureSimpleTableExistsAsync(IAmazonDynamoDB dynamoDb, string tableName)
    {
        await dynamoDb.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement("Id", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("Id", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        await WaitForTableActiveAsync(dynamoDb, tableName);
    }

    private static async Task WaitForTableActiveAsync(IAmazonDynamoDB dynamoDb, string tableName)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var desc = await dynamoDb.DescribeTableAsync(tableName);
            if (desc.Table.TableStatus == TableStatus.ACTIVE)
                return;
            await Task.Delay(200);
        }
    }
}
