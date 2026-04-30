using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using InterBus.DynamoDB.Extensions;
using InterBus.DynamoDB.Sagas;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.DynamoDB.Tests;

// ---------------------------------------------------------------------------
// Test messages
// ---------------------------------------------------------------------------

public record OrderStarted(Guid OrderId, string CustomerEmail);
public record OrderConfirmed(Guid OrderId);
public record OrderCancelled(Guid OrderId);

// ---------------------------------------------------------------------------
// Test saga state
// ---------------------------------------------------------------------------

public sealed class OrderSagaState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public bool IsCancelled { get; set; }
    public int MessageCount { get; set; }
}

// ---------------------------------------------------------------------------
// Test sagas
// ---------------------------------------------------------------------------

public sealed class OrderSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderConfirmed>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderConfirmed message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}

public sealed class OrderWithDeletionSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderCancelled>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderCancelled message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderCancelled message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsCancelled = true;
        context.Delete();
        return Task.CompletedTask;
    }
}

public sealed class CompletingOrderSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderConfirmed>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderConfirmed message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.MessageCount++;
        context.Complete();
        return Task.CompletedTask;
    }
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

[TestFixture]
public class DynamoDbSagaRepositoryTests
{
    private static readonly LocalStackContainer Container =
        new LocalStackBuilder("localstack/localstack:4").Build();
    private const string TableName = "InterBusSagasTest";

    private IAmazonDynamoDB _dynamoDb = null!;
    private DynamoDbSagaOptions _options = null!;

    [OneTimeSetUp]
    public static async Task OneTimeSetUp()
    {
        await Container.StartAsync();
    }

    [OneTimeTearDown]
    public static async Task OneTimeTearDown()
    {
        await Container.StopAsync();
        await Container.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        var awsOptions = CreateAwsOptions();
        var services = new ServiceCollection();
        services.AddAWSService<IAmazonDynamoDB>(awsOptions);
        var provider = services.BuildServiceProvider();
        _dynamoDb = provider.GetRequiredService<IAmazonDynamoDB>();

        _options = new DynamoDbSagaOptions { TableName = TableName };

        // Create the table if it doesn't exist
        await EnsureTableExistsAsync(_dynamoDb, TableName);
    }

    [TearDown]
    public void TearDown()
    {
        _dynamoDb.Dispose();
    }

    // -----------------------------------------------------------------------
    // Helper infrastructure
    // -----------------------------------------------------------------------

    private AWSOptions CreateAwsOptions() => new()
    {
        Credentials = new BasicAWSCredentials("test", "test"),
        Region = RegionEndpoint.USEast1,
        DefaultClientConfig = { ServiceURL = Container.GetConnectionString() },
    };

    private static async Task EnsureTableExistsAsync(IAmazonDynamoDB dynamoDb, string tableName)
    {
        try
        {
            await dynamoDb.DescribeTableAsync(tableName);
        }
        catch (ResourceNotFoundException)
        {
            await dynamoDb.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                KeySchema = [new KeySchemaElement(DynamoDbSagaRepository<OrderSaga, OrderSagaState>.AttrCorrelationId, KeyType.HASH)],
                AttributeDefinitions = [new AttributeDefinition(DynamoDbSagaRepository<OrderSaga, OrderSagaState>.AttrCorrelationId, ScalarAttributeType.S)],
                BillingMode = BillingMode.PAY_PER_REQUEST,
            });

            // Wait until table is active
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                var desc = await dynamoDb.DescribeTableAsync(tableName);
                if (desc.Table.TableStatus == TableStatus.ACTIVE)
                    break;
                await Task.Delay(200);
            }
        }
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 30_000,
        int intervalMs = 200)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    private DynamoDbSagaRepository<OrderSaga, OrderSagaState> CreateRepo() =>
        new(_dynamoDb, _options);

    private async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartBusAsync(
        Action<IInterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var awsOptions = CreateAwsOptions();
        services.AddAWSService<IAmazonDynamoDB>(awsOptions);

        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts);
    }

    private static async Task StopBusAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    // -----------------------------------------------------------------------
    // Repository unit tests (no full bus)
    // -----------------------------------------------------------------------

    [Test]
    public async Task GetAsync_WhenNoEntry_ReturnsNull()
    {
        var repo = CreateRepo();
        var result = await repo.GetAsync(Guid.NewGuid().ToString());
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task SaveAndGet_RoundTripsState()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var state = new OrderSagaState { CustomerEmail = "test@example.com", MessageCount = 3 };
        var entry = new SagaEntry<OrderSagaState>(state);

        await repo.SaveAsync(correlationId, entry);

        var loaded = await repo.GetAsync(correlationId);

        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.State.CustomerEmail, Is.EqualTo("test@example.com"));
        Assert.That(loaded.State.MessageCount, Is.EqualTo(3));
        Assert.That(loaded.IsCompleted, Is.False);
    }

    [Test]
    public async Task SaveAndGet_PreservesIsCompleted()
    {
        // Test via the full bus lifecycle since IsCompleted has internal set access only.
        var orderId = Guid.NewGuid();
        var tableName = $"test-{Guid.NewGuid():N}";
        await EnsureTableExistsAsync(_dynamoDb, tableName);

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<CompletingOrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = tableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "complete@example.com"));
            await publisher.PublishAsync(new OrderConfirmed(orderId));

            var repo = provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>();
            await WaitUntilAsync(() =>
                repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.IsCompleted == true);

            var loaded = await repo.GetAsync(orderId.ToString());
            Assert.That(loaded, Is.Not.Null);
            Assert.That(loaded!.IsCompleted, Is.True);
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task DeleteAsync_RemovesEntry()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var entry = new SagaEntry<OrderSagaState>(new OrderSagaState());
        await repo.SaveAsync(correlationId, entry);

        await repo.DeleteAsync(correlationId);

        var loaded = await repo.GetAsync(correlationId);
        Assert.That(loaded, Is.Null);
    }

    [Test]
    public async Task DeleteAsync_NonExistentEntry_IsNoOp()
    {
        var repo = CreateRepo();
        Assert.DoesNotThrowAsync(async () =>
            await repo.DeleteAsync(Guid.NewGuid().ToString()));
    }

    [Test]
    public async Task GetAsync_ExpiredEntry_ReturnsNull()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        // Save with an already-expired TTL
        var pastExpiry = DateTimeOffset.UtcNow.AddSeconds(-1);
        var entry = new SagaEntry<OrderSagaState>(new OrderSagaState(), pastExpiry);
        await repo.SaveAsync(correlationId, entry);

        var loaded = await repo.GetAsync(correlationId);
        Assert.That(loaded, Is.Null, "Expired entries should be treated as absent");
    }

    [Test]
    public async Task GetAsync_FutureExpiry_ReturnsEntry()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var futureExpiry = DateTimeOffset.UtcNow.AddMinutes(5);
        var entry = new SagaEntry<OrderSagaState>(new OrderSagaState { MessageCount = 7 }, futureExpiry);
        await repo.SaveAsync(correlationId, entry);

        var loaded = await repo.GetAsync(correlationId);
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.State.MessageCount, Is.EqualTo(7));
    }

    // -----------------------------------------------------------------------
    // Locking tests
    // -----------------------------------------------------------------------

    [Test]
    public async Task AcquireLock_ThenRelease_AllowsReAcquisition()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        await using (await repo.AcquireLockAsync(correlationId))
        {
            // Lock held; second acquisition should time out quickly
            var shortOpts = new DynamoDbSagaOptions
            {
                TableName = TableName,
                LockLeaseDuration = TimeSpan.FromSeconds(30),
                LockRetryInterval = TimeSpan.FromMilliseconds(100),
                LockAcquireTimeout = TimeSpan.FromMilliseconds(300),
            };
            var repo2 = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(_dynamoDb, shortOpts);
            Assert.ThrowsAsync<TimeoutException>(async () =>
                await repo2.AcquireLockAsync(correlationId));
        }

        // After release, a new acquisition should succeed immediately
        await using var _ = await repo.AcquireLockAsync(correlationId);
    }

    [Test]
    public async Task AcquireLock_AfterLeasExpiry_ClaimsAbandonedLock()
    {
        var shortLease = new DynamoDbSagaOptions
        {
            TableName = TableName,
            LockLeaseDuration = TimeSpan.FromSeconds(1),
            LockRetryInterval = TimeSpan.FromMilliseconds(100),
            LockAcquireTimeout = TimeSpan.FromSeconds(10),
        };
        var repo = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(_dynamoDb, shortLease);
        var correlationId = Guid.NewGuid().ToString();

        // Acquire a lock but don't release it (simulate a crashed process)
        _ = await repo.AcquireLockAsync(correlationId);
        // Do NOT dispose — simulating a crash

        // Wait for the lease to expire
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Another caller should be able to acquire the lock now
        await using var lock2 = await repo.AcquireLockAsync(correlationId);
        Assert.That(lock2, Is.Not.Null);
    }

    // -----------------------------------------------------------------------
    // Integration tests with full bus
    // -----------------------------------------------------------------------

    [Test]
    public async Task Saga_SingleMessage_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();
        var tableName = $"test-{Guid.NewGuid():N}";
        await EnsureTableExistsAsync(_dynamoDb, tableName);

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<OrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = tableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "test@example.com"));

            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
                return repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.MessageCount == 1;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.State.CustomerEmail, Is.EqualTo("test@example.com"));
            Assert.That(entry.State.MessageCount, Is.EqualTo(1));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Saga_MultipleMessages_StateIsAccumulated()
    {
        var orderId = Guid.NewGuid();
        var tableName = $"test-{Guid.NewGuid():N}";
        await EnsureTableExistsAsync(_dynamoDb, tableName);

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<OrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = tableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "multi@example.com"));
            await publisher.PublishAsync(new OrderConfirmed(orderId));

            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
                var e = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return e?.State.MessageCount == 2 && e.State.IsConfirmed;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.State.IsConfirmed, Is.True);
            Assert.That(entry.State.MessageCount, Is.EqualTo(2));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Saga_DeleteOnCancel_RemovesEntry()
    {
        var orderId = Guid.NewGuid();
        var tableName = $"test-{Guid.NewGuid():N}";
        await EnsureTableExistsAsync(_dynamoDb, tableName);

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<OrderWithDeletionSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = tableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "del@example.com"));
            await publisher.PublishAsync(new OrderCancelled(orderId));

            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderWithDeletionSaga, OrderSagaState>>();
                return repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult() is null;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderWithDeletionSaga, OrderSagaState>>();
            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(entry, Is.Null);
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Saga_Complete_DropsSubsequentMessages()
    {
        var orderId = Guid.NewGuid();
        var tableName = $"test-{Guid.NewGuid():N}";
        await EnsureTableExistsAsync(_dynamoDb, tableName);

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<CompletingOrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = tableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "complete@example.com"));
            await publisher.PublishAsync(new OrderConfirmed(orderId));

            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>();
                var e = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return e?.IsCompleted == true;
            });

            // A second OrderConfirmed should be dropped (saga is completed)
            var beforeCount = (await provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>()
                .GetAsync(orderId.ToString()))!.State.MessageCount;

            await publisher.PublishAsync(new OrderConfirmed(orderId));
            await Task.Delay(500); // Give time for the message to be (not) processed

            var entry = await provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>()
                .GetAsync(orderId.ToString());

            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.IsCompleted, Is.True);
            Assert.That(entry.State.MessageCount, Is.EqualTo(beforeCount), "No extra messages should be processed after completion");
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }
}
