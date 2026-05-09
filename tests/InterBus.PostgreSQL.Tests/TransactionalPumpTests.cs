using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Testcontainers.PostgreSql;
namespace InterBus.PostgreSQL.Tests;

/// <summary>
/// Verifies that <see cref="PostgreSqlMessagePumpMode.Transactional"/> still delivers
/// messages correctly and retries after consumer failures. Each test creates its own
/// DI container to prevent cross-test consumer/tracker collisions.
/// </summary>
[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class TransactionalPumpTests(string imageTag)
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();

    [OneTimeSetUp]
    public Task OneTimeSetUp() => _container.StartAsync();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  1. Publish fan-out reaches all subscribers
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Transactional_Publish_AllSubscribersReceive()
    {
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        await using var host = await BuildAndStartAsync(
            svc =>
            {
                svc.AddSingleton(orderTracker);
                svc.AddSingleton(auditTracker);
            },
            bus =>
            {
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<AuditConsumer>();
            });

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "tx@test.com", 10m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(orderTracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            Assert.That(auditTracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  2. Failed consumer is dead-lettered (not retried)
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Transactional_FailedConsumer_MessageIsMovedToDeadLetterTable()
    {
        var receiveCounter = new TxReceiveCounter();

        await using var host = await BuildAndStartAsync(
            svc => svc.AddSingleton(receiveCounter),
            bus => bus.AddConsumer<TransactionalFailingConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "tx-dlq@test.com", 20m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        // Wait for the consumer to be called once.
        await TestHelpers.WaitUntilAsync(() => receiveCounter.CallCount >= 1, timeoutMs: 15_000);

        // Allow a short window for the pump to commit the dead-letter transaction.
        await Task.Delay(500, TestContext.CurrentContext.CancellationToken);

        var dataSource = scope.ServiceProvider.GetRequiredService<NpgsqlDataSource>();
        var deadLetterCount = await CountDeadLetterRowsAsync(dataSource);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(deadLetterCount, Is.EqualTo(1), "Expected exactly one row in dead_letter_messages");
            Assert.That(receiveCounter.CallCount, Is.EqualTo(1), "Failed message should be dead-lettered, not retried");
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  3. Consumer can access and use the shared transaction
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Transactional_Consumer_CanAccessSharedTransaction()
    {
        var transactionTracker = new TransactionTracker();

        await using var host = await BuildAndStartAsync(
            svc => svc.AddSingleton(transactionTracker),
            bus => bus.AddConsumer<TransactionAccessConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "tx-access@test.com", 5m),
            TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => transactionTracker.ReceivedTransaction is not null);

        Assert.That(transactionTracker.ReceivedTransaction, Is.Not.Null,
            "Consumer should have received a non-null NpgsqlTransaction via GetPostgreSqlTransaction()");
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  4. Each dispatch receives a non-null transaction
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Transactional_EachDispatch_ReceivesNonNullTransaction()
    {
        // Verify that every dispatch exposes a non-null transaction to the consumer.
        var transactionSlots = new TransactionSlotTracker();

        await using var host = await BuildAndStartAsync(
            svc => svc.AddSingleton(transactionSlots),
            bus => bus.AddConsumer<TransactionSlotConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var ct = TestContext.CurrentContext.CancellationToken;

        // Publish two messages so we get two dispatches.
        await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "slot1@test.com", 1m), ct);
        await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "slot2@test.com", 2m), ct);

        await TestHelpers.WaitUntilAsync(() => transactionSlots.Snapshots.Count >= 2);

        Assert.That(transactionSlots.Snapshots,
            Has.All.Not.Null,
            "Every dispatch should expose a non-null transaction to the consumer");
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Helper
    // ──────────────────────────────────────────────────────────────────────────

    private async Task<TestHost> BuildAndStartAsync(
        Action<IServiceCollection> configureServices,
        Action<IInterBusBuilder> configureConsumers)
    {
        var cts = new CancellationTokenSource();
        var services = new ServiceCollection();
        services.AddLogging();
        configureServices(services);

        services.AddInterBus(bus =>
        {
            bus.AddPostgreSqlTransport(o =>
            {
                o.ConnectionString = _container.GetConnectionString();
                o.ConnectionRetry.RetryCount = 5;
                o.MessagePumpMode = PostgreSqlMessagePumpMode.Transactional;
            });
            configureConsumers(bus);
        });

        var provider = services.BuildServiceProvider();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return new TestHost(provider, cts);
    }

    private static async Task<int> CountDeadLetterRowsAsync(NpgsqlDataSource dataSource)
    {
        await using var cmd = dataSource.CreateCommand("SELECT COUNT(*) FROM interbus.dead_letter_messages");
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Inner consumer types
    // ──────────────────────────────────────────────────────────────────────────

    internal sealed class TransactionalFailingConsumer(TxReceiveCounter receiveCounter) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            receiveCounter.Increment();
            throw new InvalidOperationException("transient transactional failure");
        }
    }

    internal sealed class TxReceiveCounter
    {
        private int _callCount;
        public int CallCount => _callCount;
        public int Increment() => Interlocked.Increment(ref _callCount);
    }

    internal sealed class TransactionTracker
    {
        public NpgsqlTransaction? ReceivedTransaction { get; private set; }
        public void Record(NpgsqlTransaction? tx) => ReceivedTransaction = tx;
    }

    internal sealed class TransactionAccessConsumer(TransactionTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Record(context.GetPostgreSqlTransaction());
            return Task.CompletedTask;
        }
    }

    internal sealed class TransactionSlotTracker
    {
        private readonly System.Collections.Concurrent.ConcurrentBag<NpgsqlTransaction?> _snapshots = [];
        public IReadOnlyCollection<NpgsqlTransaction?> Snapshots => _snapshots;
        public void Record(NpgsqlTransaction? tx) => _snapshots.Add(tx);
    }

    internal sealed class TransactionSlotConsumer(TransactionSlotTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Record(context.GetPostgreSqlTransaction());
            return Task.CompletedTask;
        }
    }

    private sealed class TestHost(ServiceProvider provider, CancellationTokenSource cts) : IAsyncDisposable
    {
        public IServiceScope CreateScope() => provider.CreateScope();

        public async ValueTask DisposeAsync()
        {
            await cts.CancelAsync();
            foreach (var svc in provider.GetServices<IHostedService>())
                await svc.StopAsync(CancellationToken.None);
            cts.Dispose();
            await provider.DisposeAsync();
        }
    }
}
