using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class PublishSubscribeSingleConsumerTests(string imageTag)
{
    private PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _orderTracker;

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
        _orderTracker = new OrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);
        services.AddInterBus(bus =>
        {
            bus.AddPostgreSqlTransport(o =>
            {
                o.ConnectionString = _container.GetConnectionString();
                o.ConnectionRetry.RetryCount = 5;
            });
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.AddConsumer<AuditConsumer>();
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
    public async Task Publish_SingleConsumer_MessageIsDelivered()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);
        await publisher.Publish(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _orderTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_orderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_orderTracker.Messages.Single().OrderId));
        }
    }
}
