using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;

namespace InterBus.PostgreSQL.Tests;

/// <summary>
/// Lease-pump specific integration tests.
/// Uses short lease durations to exercise renewal, release-on-failure, and expired-lease reclaim
/// within reasonable time bounds.
/// </summary>
[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class LeasePumpTests(string imageTag)
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
    //  1. Basic delivery – lease pump delivers a message end-to-end
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Lease_Publish_MessageIsDelivered()
    {
        var tracker = new OrderPlacedTracker();
        await using var host = await BuildAndStartAsync(
            o => o.MessagePumpMode = PostgreSqlMessagePumpMode.Lease,
            svc => svc.AddSingleton(tracker),
            bus => bus.AddConsumer<OrderPlacedConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "lease@test.com", 50m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => tracker.Messages.Count == 1);
        Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  2. Failure releases immediately – message is retried before lease expiry
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Lease_FailedConsumer_IsReleasedImmediatelyAndRetried()
    {
        var tracker = new OrderPlacedTracker();
        var counter = new LeaseAtomicCounter();

        // 30 s lease; release_claim must make the message visible again immediately on
        // failure so the retry arrives well within the 15 s test window.
        await using var host = await BuildAndStartAsync(
            o =>
            {
                o.MessagePumpMode = PostgreSqlMessagePumpMode.Lease;
                o.LeaseDuration = TimeSpan.FromSeconds(30);
                o.FallbackPollInterval = TimeSpan.FromMilliseconds(200);
                o.FailureRedeliveryDelay = TimeSpan.Zero;
            },
            svc =>
            {
                svc.AddSingleton(tracker);
                svc.AddSingleton(counter);
            },
            bus => bus.AddConsumer<LeaseFailThenSucceedConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "release@test.com", 5m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(
            () => tracker.Messages.Count == 1,
            timeoutMs: 15_000);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            Assert.That(counter.Value, Is.GreaterThanOrEqualTo(2));
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  3. Lease expiry – if a handler outruns its lease, the message is
    //     re-delivered and the original stale ack is safely rejected
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Lease_StalledHandler_MessageIsReclaimedAfterExpiry()
    {
        var tracker = new OrderPlacedTracker();
        var counter = new LeaseAtomicCounter();

        // 3 s lease, renewal interval 60 s (will never fire in this test), 500 ms fallback poll.
        // On attempt 1 the consumer sleeps for 8 s (> 3 s lease) so the lease expires mid-handler.
        // After the sleep the pump retries the now-visible row; attempt 2 succeeds.
        await using var host = await BuildAndStartAsync(
            o =>
            {
                o.MessagePumpMode = PostgreSqlMessagePumpMode.Lease;
                o.LeaseDuration = TimeSpan.FromSeconds(3);
                o.LeaseRenewInterval = TimeSpan.FromSeconds(60);
                o.FallbackPollInterval = TimeSpan.FromMilliseconds(500);
                o.FailureRedeliveryDelay = TimeSpan.Zero;
            },
            svc =>
            {
                svc.AddSingleton(tracker);
                svc.AddSingleton(counter);
            },
            bus => bus.AddConsumer<LeaseStallingConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "expire@test.com", 99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(
            () => tracker.Messages.Count == 1,
            timeoutMs: 30_000);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
            Assert.That(counter.Value, Is.GreaterThanOrEqualTo(2),
                "Should have been delivered at least twice: once while stalling and once after lease expiry");
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Helper
    // ──────────────────────────────────────────────────────────────────────────

    private async Task<TestHost> BuildAndStartAsync(
        Action<PostgreSqlTransportOptions> configure,
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
                configure(o);
            });
            configureConsumers(bus);
        });

        var provider = services.BuildServiceProvider();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return new TestHost(provider, cts);
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Inner consumer types (source generator picks up all IConsumer<T> in project)
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>Fails on the first attempt; succeeds from the second onward.</summary>
    public sealed class LeaseFailThenSucceedConsumer(OrderPlacedTracker tracker, LeaseAtomicCounter counter)
        : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var attempt = counter.Increment();
            if (attempt < 2)
                throw new InvalidOperationException("intentional failure to exercise release_claim");

            tracker.Track(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Sleeps for 8 s on the first attempt, allowing the 3 s lease to expire. On subsequent
    /// attempts it succeeds immediately. Uses <see cref="CancellationToken.None"/> for the
    /// sleep so that host shutdown does not interrupt the stall and corrupt the test.
    /// </summary>
    public sealed class LeaseStallingConsumer(OrderPlacedTracker tracker, LeaseAtomicCounter counter)
        : IConsumer<OrderPlaced>
    {
        private static readonly TimeSpan StallingDuration = TimeSpan.FromSeconds(8);

        public async Task Consume(OrderPlaced message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            var attempt = counter.Increment();
            if (attempt == 1)
            {
                // Sleep past the lease duration. The lease expires at 3 s; we stall 8 s.
                // Use CancellationToken.None so teardown does not cut short the stall while
                // the test is still waiting on the reclaim path.
                await Task.Delay(StallingDuration, CancellationToken.None);
                // Return without tracking – ack_claim will be rejected by token mismatch.
                return;
            }

            tracker.Track(message);
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Shared DI-injectable helpers
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>Thread-safe attempt counter injected into consumers as a singleton.</summary>
    public sealed class LeaseAtomicCounter
    {
        private int _value;
        public int Value => _value;

        /// <returns>The new value after incrementing.</returns>
        public int Increment() => Interlocked.Increment(ref _value);
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
