using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class FailedConsumerRetriesTests(string imageTag)
{
    private PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _tracker;
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
        _tracker = new OrderPlacedTracker();
        _receiveTracker = new ReceiveTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_tracker);
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
    public async Task FailedConsumer_MessageIsRetried()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "retry@test.com", 10m);
        await publisher.Publish(msg, TestContext.CurrentContext.CancellationToken);

        // Wait for the second (successful) delivery.
        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count == 1, timeoutMs: 15_000);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(_tracker.Messages, Has.All.Property(nameof(OrderPlaced.OrderId)).EqualTo(msg.OrderId));
            Assert.That(_receiveTracker.CallCount, Is.GreaterThanOrEqualTo(2));
        }
    }

    internal sealed class FailingOrderPlacedConsumer(ReceiveTracker receiveTracker, OrderPlacedTracker tracker)
        : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            receiveTracker.Increment();

            if (receiveTracker.CallCount < 2)
                throw new InvalidOperationException("transient failure");

            tracker.Track(message);

            return Task.CompletedTask;
        }
    }

    internal sealed class ReceiveTracker
    {
        private int _callCount;
        public int CallCount => _callCount;
        public void Increment() => Interlocked.Increment(ref _callCount);
    }
}
