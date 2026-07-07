using System.Collections.Concurrent;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.DynamoDB.Extensions;
using InterBus.DynamoDB.Outbox;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Retry;
using InterBus.Sending;
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

public sealed class OutboxConsumerExecutionTracker
{
    private int _count;
    public int Count => _count;
    public void Track() => Interlocked.Increment(ref _count);
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

public sealed class OutboxInboxPublishingConsumer(
    OutboxConsumerExecutionTracker executionTracker,
    IPublisher publisher)
    : IConsumer<OutboxOrderPlaced>
{
    public async Task Consume(OutboxOrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        executionTracker.Track();
        await publisher.PublishAsync(new OutboxOrderShipped(message.OrderId), cancellationToken);
    }
}

public sealed class OutboxInboxExecutionOnlyConsumer(OutboxConsumerExecutionTracker executionTracker)
    : IConsumer<OutboxOrderPlaced>
{
    public Task Consume(OutboxOrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        executionTracker.Track();
        return Task.CompletedTask;
    }
}

public sealed class OutboxInboxFailFirstConsumer(
    OutboxConsumerExecutionTracker executionTracker,
    FailFirstConsumeState state)
    : IConsumer<OutboxOrderPlaced>
{
    public Task Consume(OutboxOrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        executionTracker.Track();
        if (Interlocked.Increment(ref state.CallCount) == 1)
            throw new Exception("Simulated consumer failure");

        return Task.CompletedTask;
    }
}

public sealed class FailFirstConsumeState
{
    public int CallCount;
}

internal sealed class FailFirstSendMiddleware(FailFirstSendState state) : ISendMiddleware
{
    public Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        if (!context.Destination.ToString().Contains(nameof(OutboxOrderShipped), StringComparison.OrdinalIgnoreCase))
            return next(context, cancellationToken);

        if (Interlocked.Increment(ref state.CallCount) == 1)
            throw new Exception("Simulated send failure");

        return next(context, cancellationToken);
    }
}

internal sealed class FailFirstSendState
{
    public int CallCount;
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
    private string _inboxTableName = string.Empty;

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
        _inboxTableName = $"{runId}-InterBusInboxTest";

        var awsOptions = TestHelpers.CreateAwsOptions(_container);
        var services = new ServiceCollection();
        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);
        services.AddAWSService<IAmazonDynamoDB>();

        var provider = services.BuildServiceProvider();
        _dynamoDb = provider.GetRequiredService<IAmazonDynamoDB>();

        await DynamoDbOutboxTable.EnsureTableCreatedAsync(_dynamoDb, _tableName,
            new DynamoDbOutboxOptions().GsiIndexName);
        await DynamoDbInboxTable.EnsureTableCreatedAsync(_dynamoDb, _inboxTableName);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _dynamoDb.DeleteTableAsync(_tableName);
        await _dynamoDb.DeleteTableAsync(_inboxTableName);
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
            bus.UseDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.PollInterval = TimeSpan.FromHours(1);
                o.RunOutboxProcessor = false;
            });
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
                ConsistentRead = true,
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
            bus.UseDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.PollInterval = TimeSpan.FromMilliseconds(50);
            });
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

            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            Assert.That(tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(tracker.Messages.Single().CustomerEmail, Is.EqualTo("deliver@example.com"));

            // Processor should have deleted the outbox items after forwarding.
            var scanResponse = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
                ConsistentRead = true,
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
            bus.UseDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.PollInterval = TimeSpan.FromMilliseconds(50);
            });
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

            await WaitUntilAsync(() => tracker.Messages.Count == 2);

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
            bus.AddDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.PollInterval = TimeSpan.FromMilliseconds(50);
            });
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

            await WaitUntilAsync(() => shippedTracker.Messages.Count == 1);

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
            bus.UseDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.PollInterval = TimeSpan.FromHours(1);
                o.RunOutboxProcessor = false;
            });
        }, services => services.AddSingleton(tracker));

        try
        {
            var businessId = Guid.NewGuid().ToString("N");

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var outboxContext = scope.ServiceProvider.GetRequiredService<IDynamoDbOutboxContext>();

                // Register a business DynamoDB write.
                outboxContext.AddTransactItem(new TransactWriteItem
                {
                    Put = new Put
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
                ConsistentRead = true,
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
    /// Inbox+outbox flow should run the consumer once and, on redelivery, resume outbox delivery
    /// without rerunning the consumer.
    /// </summary>
    [Test]
    public async Task Consumer_UseInboxOutbox_RedeliverySkipsConsumerAndClearsOutbox()
    {
        const string completedInboxState = "2";
        var shippedTracker = new OutboxOrderShippedTracker();
        var executionTracker = new OutboxConsumerExecutionTracker();
        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxInboxPublishingConsumer>(c =>
            {
                c.UseRetryMiddleware(options =>
                {
                    options.RetryCount = 2;
                    options.DelayProvider = RetryDelays.Constant(TimeSpan.FromMilliseconds(100));
                });
                c.UseDynamoDbInboxOutbox();
            });
            bus.AddConsumer<OutboxOrderShippedConsumer>();
            bus.AddDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.InboxTableName = _inboxTableName;
                o.RunOutboxProcessor = false;
                o.PollInterval = TimeSpan.FromHours(1);
            });
            bus.UseSendMiddleware<FailFirstSendMiddleware>();
        }, services =>
        {
            services.AddSingleton(shippedTracker);
            services.AddSingleton(executionTracker);
            services.AddSingleton<FailFirstSendState>();
        });

        try
        {
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "inbox-outbox@example.com"),
                    CancellationToken.None);
            }

            await WaitUntilAsync(() => shippedTracker.Messages.Count > 0, timeoutMs: 10_000);

            using (Assert.EnterMultipleScope())
            {
                Assert.That(executionTracker.Count, Is.EqualTo(1));
                Assert.That(shippedTracker.Messages, Has.Count.EqualTo(1));
            }

            var outboxScan = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
                ConsistentRead = true,
            }, CancellationToken.None);

            Assert.That(outboxScan.Count, Is.Zero);

            var inboxScan = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _inboxTableName,
                ConsistentRead = true,
            }, CancellationToken.None);

            Assert.That(inboxScan.Count, Is.EqualTo(1));
            Assert.That(inboxScan.Items.Single()[DynamoDbInboxTable.State].N, Is.EqualTo(completedInboxState));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Consumer_UseInboxOutbox_WithoutMessageIdHeader_BypassesInbox()
    {
        var executionTracker = new OutboxConsumerExecutionTracker();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxInboxExecutionOnlyConsumer>(c => c.UseDynamoDbInboxOutbox());
            bus.AddDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.InboxTableName = _inboxTableName;
                o.RunOutboxProcessor = false;
                o.PollInterval = TimeSpan.FromHours(1);
            });
        }, services => services.AddSingleton(executionTracker));

        try
        {
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "missing-id-1@example.com"),
                    ctx => ctx.Headers.Remove(InterBusHeaders.MessageId),
                    CancellationToken.None);

                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "missing-id-2@example.com"),
                    ctx => ctx.Headers.Remove(InterBusHeaders.MessageId),
                    CancellationToken.None);
            }

            await WaitUntilAsync(() => executionTracker.Count == 2);

            Assert.That(executionTracker.Count, Is.EqualTo(2));

            var inboxScan = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _inboxTableName,
                ConsistentRead = true,
            }, CancellationToken.None);
            Assert.That(inboxScan.Count, Is.EqualTo(0));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Consumer_UseInboxOutbox_ConsumerFailureThenRetry_CompletesInboxRow()
    {
        const string completedInboxState = "2";
        var executionTracker = new OutboxConsumerExecutionTracker();
        var failFirstState = new FailFirstConsumeState();
        var messageId = Guid.NewGuid().ToString("N");
        var consumerName = typeof(OutboxInboxFailFirstConsumer).FullName!;

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxInboxFailFirstConsumer>(c =>
            {
                c.UseRetryMiddleware(options =>
                {
                    options.RetryCount = 1;
                    options.DelayProvider = RetryDelays.Constant(TimeSpan.FromMilliseconds(100));
                });
                c.UseDynamoDbInboxOutbox();
            });
            bus.AddDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.InboxTableName = _inboxTableName;
                o.RunOutboxProcessor = false;
                o.PollInterval = TimeSpan.FromHours(1);
            });
        }, services =>
        {
            services.AddSingleton(executionTracker);
            services.AddSingleton(failFirstState);
        });

        try
        {
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "retry@example.com"),
                    ctx => ctx.Headers[InterBusHeaders.MessageId] = messageId,
                    CancellationToken.None);
            }

            await WaitUntilAsync(() => executionTracker.Count == 2, timeoutMs: 10000);
            await WaitForInboxStateAsync(consumerName, messageId, completedInboxState, timeoutMs: 10000);

            Assert.That(executionTracker.Count, Is.EqualTo(2));

            var inboxItem = await _dynamoDb.GetItemAsync(new GetItemRequest
            {
                TableName = _inboxTableName,
                Key = BuildInboxKey(consumerName, messageId),
                ConsistentRead = true,
            }, CancellationToken.None);

            Assert.That(inboxItem.IsItemSet, Is.True);
            Assert.That(inboxItem.Item[DynamoDbInboxTable.State].N, Is.EqualTo(completedInboxState));

            var outboxScan = await _dynamoDb.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
                ConsistentRead = true,
            }, CancellationToken.None);
            Assert.That(outboxScan.Count, Is.EqualTo(0));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Consumer_UseInboxOutbox_PendingOutboxWithoutGroupId_MarksInboxCompleted()
    {
        const string pendingOutboxState = "1";
        const string completedInboxState = "2";
        var executionTracker = new OutboxConsumerExecutionTracker();
        var messageId = Guid.NewGuid().ToString("N");
        var consumerName = typeof(OutboxInboxExecutionOnlyConsumer).FullName!;

        await _dynamoDb.PutItemAsync(new PutItemRequest
        {
            TableName = _inboxTableName,
            Item = new Dictionary<string, AttributeValue>
            {
                [DynamoDbInboxTable.ConsumerName] = new() { S = consumerName },
                [DynamoDbInboxTable.MessageId] = new() { S = messageId },
                [DynamoDbInboxTable.State] = new() { N = pendingOutboxState },
                [DynamoDbInboxTable.CreatedAt] = new()
                    { N = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString() },
            },
        }, CancellationToken.None);

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OutboxInboxExecutionOnlyConsumer>(c => c.UseDynamoDbInboxOutbox());
            bus.AddDynamoDbOutbox(o =>
            {
                o.TableName = _tableName;
                o.InboxTableName = _inboxTableName;
                o.RunOutboxProcessor = false;
                o.PollInterval = TimeSpan.FromHours(1);
            });
        }, services => services.AddSingleton(executionTracker));

        try
        {
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                await publisher.PublishAsync(
                    new OutboxOrderPlaced(Guid.NewGuid(), "pending-outbox-no-group@example.com"),
                    ctx => ctx.Headers[InterBusHeaders.MessageId] = messageId,
                    CancellationToken.None);
            }

            await WaitForInboxStateAsync(consumerName, messageId, completedInboxState, timeoutMs: 10000);

            Assert.That(executionTracker.Count, Is.EqualTo(0));

            var inboxResponse = await _dynamoDb.GetItemAsync(new GetItemRequest
            {
                TableName = _inboxTableName,
                Key = BuildInboxKey(consumerName, messageId),
                ConsistentRead = true,
            }, CancellationToken.None);

            Assert.That(inboxResponse.IsItemSet, Is.True);
            Assert.That(inboxResponse.Item[DynamoDbInboxTable.State].N, Is.EqualTo(completedInboxState));
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
        Action<InterBusBuilder> configureBus,
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

    private static async Task EnsureSimpleTableExistsAsync(IAmazonDynamoDB dynamoDb, string tableName)
    {
        await dynamoDb.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema = [new KeySchemaElement("Id", KeyType.HASH)],
            AttributeDefinitions = [new AttributeDefinition("Id", ScalarAttributeType.S)],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        });

        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var desc = await dynamoDb.DescribeTableAsync(tableName);
            if (desc.Table.TableStatus == TableStatus.ACTIVE)
                return;
            await Task.Delay(200);
        }
    }

    private async Task WaitForInboxStateAsync(
        string consumerName,
        string messageId,
        string expectedState,
        int timeoutMs = 30_000,
        int intervalMs = 200)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            var response = await _dynamoDb.GetItemAsync(new GetItemRequest
            {
                TableName = _inboxTableName,
                Key = BuildInboxKey(consumerName, messageId),
                ConsistentRead = true,
            }, CancellationToken.None);

            if (response.IsItemSet &&
                response.Item.TryGetValue(DynamoDbInboxTable.State, out var state) &&
                state.N == expectedState)
                return;

            await Task.Delay(intervalMs);
        }

        Assert.Fail($"Inbox row for '{consumerName}' / '{messageId}' did not reach state '{expectedState}'.");
    }

    private static Dictionary<string, AttributeValue> BuildInboxKey(string consumerName, string messageId) =>
        new()
        {
            [DynamoDbInboxTable.ConsumerName] = new() { S = consumerName },
            [DynamoDbInboxTable.MessageId] = new() { S = messageId },
        };
}
