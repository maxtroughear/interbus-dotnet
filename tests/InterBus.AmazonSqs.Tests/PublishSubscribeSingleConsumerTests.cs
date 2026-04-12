using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class PublishSubscribeSingleConsumerTests
{
    private static readonly LocalStackContainer Container =
        new LocalStackBuilder("localstack/localstack:latest").Build();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private OrderPlacedTracker _orderTracker = null!;

    [OneTimeSetUp]
    public static Task OneTimeSetUp() => Container.StartAsync();

    [OneTimeTearDown]
    public static async Task OneTimeTearDown()
    {
        await Container.StopAsync();
        await Container.DisposeAsync();
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
            bus.AddAmazonSqsTransport(o =>
            {
                o.ServiceUrl = Container.GetConnectionString();
                o.AccessKey = "test";
                o.SecretKey = "test";
                o.Region = "us-east-1";
            });
            bus.AddConsumer<OrderPlacedConsumer>();
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

    [Test]
    public async Task Send_DirectQueue_MessageIsDelivered()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "direct@example.com", 49.99m);
        await publisher.Send<OrderPlacedConsumer, OrderPlaced>(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _orderTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_orderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_orderTracker.Messages.Single().OrderId));
        }
    }
}
