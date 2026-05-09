using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Testcontainers.PostgreSql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class DeadLetteringTests(string imageTag)
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
    //  Lease pump: dead-letter test
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task LeasePump_FailedConsumer_MessageIsMovedToDeadLetterTable()
    {
        var counter = new DeadLetterReceiveCounter();

        await using var host = await BuildAndStartAsync(
            o => o.MessagePumpMode = PostgreSqlMessagePumpMode.Lease,
            svc => svc.AddSingleton(counter),
            bus => bus.AddConsumer<DeadLetterOrderPlacedConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "dlq-lease@test.com", 1m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        // Wait until the consumer has been called.
        await TestHelpers.WaitUntilAsync(() => counter.Count >= 1, timeoutMs: 15_000);

        // Allow a short window for the pump to write to dead_letter_messages.
        await Task.Delay(500, TestContext.CurrentContext.CancellationToken);

        var row = await ReadDeadLetterRowAsync(host);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(row, Is.Not.Null, "Expected a row in dead_letter_messages");
            Assert.That(row!.QueueName, Does.Contain(nameof(DeadLetterOrderPlacedConsumer)));
            Assert.That(row.Reason, Is.EqualTo("Unhandled exception in consume pipeline"));
            Assert.That(row.Error, Does.Contain("Simulated dead-letter failure"));
            Assert.That(row.DeliveryCount, Is.EqualTo(1));
            Assert.That(counter.Count, Is.EqualTo(1), "Message should be dead-lettered on the first attempt, not retried");
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Transactional pump: dead-letter test
    // ──────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task TransactionalPump_FailedConsumer_MessageIsMovedToDeadLetterTable()
    {
        var counter = new DeadLetterReceiveCounter();

        await using var host = await BuildAndStartAsync(
            o => o.MessagePumpMode = PostgreSqlMessagePumpMode.Transactional,
            svc => svc.AddSingleton(counter),
            bus => bus.AddConsumer<DeadLetterOrderPlacedConsumer>());

        using var scope = host.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "dlq-tx@test.com", 2m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        // Wait until the consumer has been called.
        await TestHelpers.WaitUntilAsync(() => counter.Count >= 1, timeoutMs: 15_000);

        // Allow a short window for the pump to commit the dead-letter transaction.
        await Task.Delay(500, TestContext.CurrentContext.CancellationToken);

        var row = await ReadDeadLetterRowAsync(host);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(row, Is.Not.Null, "Expected a row in dead_letter_messages");
            Assert.That(row!.QueueName, Does.Contain(nameof(DeadLetterOrderPlacedConsumer)));
            Assert.That(row.Reason, Is.EqualTo("Unhandled exception in consume pipeline"));
            Assert.That(row.Error, Does.Contain("Simulated dead-letter failure"));
            Assert.That(counter.Count, Is.EqualTo(1), "Message should be dead-lettered on the first attempt, not retried");
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
                o.FallbackPollInterval = TimeSpan.FromMilliseconds(200);
                configure(o);
            });
            configureConsumers(bus);
        });

        var provider = services.BuildServiceProvider();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return new TestHost(provider, cts);
    }

    private static async Task<DeadLetterRow?> ReadDeadLetterRowAsync(TestHost host)
    {
        using var scope = host.CreateScope();
        var dataSource = scope.ServiceProvider.GetRequiredService<NpgsqlDataSource>();
        await using var cmd = dataSource.CreateCommand(
            "SELECT queue_name, reason, error, dead_lettered_at, delivery_count FROM interbus.dead_letter_messages LIMIT 1");
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new DeadLetterRow(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetFieldValue<DateTimeOffset>(3),
            reader.GetInt32(4));
    }

    // ──────────────────────────────────────────────────────────────────────────
    //  Inner types
    // ──────────────────────────────────────────────────────────────────────────

    /// <summary>Thread-safe counter that tracks how many times the dead-letter consumer was invoked.</summary>
    public sealed class DeadLetterReceiveCounter
    {
        private int _count;
        public int Count => _count;
        public void Increment() => Interlocked.Increment(ref _count);
    }

    public sealed class DeadLetterOrderPlacedConsumer(DeadLetterReceiveCounter counter) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            counter.Increment();
            throw new InvalidOperationException("Simulated dead-letter failure");
        }
    }

    private sealed record DeadLetterRow(
        string QueueName,
        string Reason,
        string Error,
        DateTimeOffset DeadLetteredAt,
        int DeliveryCount);

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
