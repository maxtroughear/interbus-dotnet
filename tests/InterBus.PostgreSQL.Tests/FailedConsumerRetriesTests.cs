using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Testcontainers.PostgreSql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class FailedConsumerRetriesTests(string imageTag)
{
    private PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private ReceiveTracker _receiveTracker;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    [SetUp]
    public async Task Setup()
    {
        _receiveTracker = new ReceiveTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_receiveTracker);
        services.AddInterBus(bus =>
        {
            bus.AddPostgreSqlTransport(o =>
            {
                o.ConnectionString = _container.GetConnectionString();
                o.ConnectionRetry.RetryCount = 5;
            });
            bus.AddConsumer<FailingOrderPlacedConsumer>();
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _cts.CancelAsync();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StopAsync(_cts.Token);

        _cts.Dispose();
        await _provider.DisposeAsync();
    }

    [Test]
    public async Task FailedConsumer_MessageIsMovedToDeadLetterTable()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "deadletter@test.com", 10m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        // Wait for the consumer to be called once.
        await TestHelpers.WaitUntilAsync(() => _receiveTracker.CallCount >= 1, timeoutMs: 15_000);

        // Allow a short window for the pump to write the dead-letter row.
        await Task.Delay(500, TestContext.CurrentContext.CancellationToken);

        var dataSource = scope.ServiceProvider.GetRequiredService<NpgsqlDataSource>();
        var deadLetterCount = await CountDeadLetterRowsAsync(dataSource);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(deadLetterCount, Is.EqualTo(1), "Expected exactly one row in dead_letter_messages");
            Assert.That(_receiveTracker.CallCount, Is.EqualTo(1), "Failed message should be dead-lettered, not retried");
        }
    }

    private static async Task<int> CountDeadLetterRowsAsync(NpgsqlDataSource dataSource)
    {
        await using var cmd = dataSource.CreateCommand("SELECT COUNT(*) FROM interbus.dead_letter_messages");
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    internal sealed class FailingOrderPlacedConsumer(ReceiveTracker receiveTracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            receiveTracker.Increment();
            throw new InvalidOperationException("transient failure");
        }
    }

    internal sealed class ReceiveTracker
    {
        private int _callCount;
        public int CallCount => _callCount;
        public void Increment() => Interlocked.Increment(ref _callCount);
    }
}
