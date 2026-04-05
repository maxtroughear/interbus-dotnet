using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "tx@test.com", 10m);
        await publisher.Publish(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(
            () => orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(orderTracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            Assert.That(auditTracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  2. Failed consumer is retried after transaction rollback
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Transactional_FailedConsumer_MessageIsRetried()
    {
        var retryTracker = new OrderPlacedTracker();
        var receiveCounter = new TxReceiveCounter();

        await using var host = await BuildAndStartAsync(
            svc =>
            {
                svc.AddSingleton(retryTracker);
                svc.AddSingleton(receiveCounter);
            },
            bus => bus.AddConsumer<TransactionalFailingConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "tx-retry@test.com", 20m);
        await publisher.Publish(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(
            () => retryTracker.Messages.Count == 1,
            timeoutMs: 15_000);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(retryTracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            Assert.That(receiveCounter.CallCount, Is.GreaterThanOrEqualTo(2));
        }
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

    // ──────────────────────────────────────────────────────────────────────────
    //  Inner consumer types
    // ──────────────────────────────────────────────────────────────────────────

    internal sealed class TransactionalFailingConsumer(
        OrderPlacedTracker retryTracker,
        TxReceiveCounter receiveCounter)
        : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var attempt = receiveCounter.Increment();
            if (attempt < 2)
                throw new InvalidOperationException("transient transactional failure");

            retryTracker.Track(message);
            return Task.CompletedTask;
        }
    }

    internal sealed class TxReceiveCounter
    {
        private int _callCount;
        public int CallCount => _callCount;
        public int Increment() => Interlocked.Increment(ref _callCount);
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
