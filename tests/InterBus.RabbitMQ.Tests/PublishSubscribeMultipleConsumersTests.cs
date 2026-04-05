using InterBus.Extensions;
using InterBus.Generated;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.RabbitMq;

namespace InterBus.RabbitMQ.Tests;

[TestFixtureSource(typeof(RabbitMqVersions), nameof(RabbitMqVersions.All))]
public class PublishSubscribeMultipleConsumersTests(string imageTag)
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder(imageTag).Build();
    private ServiceProvider _provider;
    private CancellationTokenSource _cts;
    private OrderPlacedTracker _orderTracker;
    private AuditTracker _auditTracker;

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
        _auditTracker = new AuditTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);
        services.AddSingleton(_auditTracker);
        services.AddInterBus(bus =>
        {
            bus.AddRabbitMqTransport(o =>
            {
                o.Uri = _container.GetConnectionString();
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
            await svc.StopAsync(CancellationToken.None);

        _cts.Dispose();
        await _provider.DisposeAsync();
    }

    [Test]
    public async Task Publish_MultipleConsumers_AllReceiveMessage()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "fan@out.test", 50m);
        await publisher.Publish(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(
            () => _orderTracker.Messages.Count == 1 && _auditTracker.Messages.Count == 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_orderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_orderTracker.Messages.Single().OrderId));
            Assert.That(_auditTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_auditTracker.Messages.Single().OrderId));
        }
    }

    [Test]
    public async Task Send_DirectToQueue_OnlyTargetConsumerReceives()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "direct@send.test", 25m);

        await publisher.Send<OrderPlacedConsumer, OrderPlaced>(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _orderTracker.Messages.Count == 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_orderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_orderTracker.Messages.Single().OrderId));
            Assert.That(_auditTracker.Messages, Is.Empty);
        }
    }
}
