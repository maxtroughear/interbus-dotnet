using Amazon.Extensions.NETCore.Setup;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class PublishSubscribeMultipleConsumersTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private OrderPlacedTracker _orderTracker = null!;
    private AuditTracker _auditTracker = null!;
    private string _runId = null!;

    [OneTimeSetUp]
    public static async Task OneTimeSetUp()
    {
        if (Container is not null)
            await Container.StartAsync();
    }

    [OneTimeTearDown]
    public static async Task OneTimeTearDown()
    {
        if (Container is not null)
        {
            await Container.StopAsync();
            await Container.DisposeAsync();
        }
    }

    [SetUp]
    public async Task Setup()
    {
        _runId = Guid.NewGuid().ToString("N")[..8];
        _orderTracker = new OrderPlacedTracker();
        _auditTracker = new AuditTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);
        services.AddSingleton(_auditTracker);

        AWSOptions awsOptions = TestHelpers.CreateAwsOptions(Container);
        services.AddAWSService<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(awsOptions);
        services.AddAWSService<Amazon.SQS.IAmazonSQS>(awsOptions);

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                o.QueuePrefix = $"q-{_runId}-";
                o.TopicPrefix = $"t-{_runId}-";
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
    public async Task Publish_MultipleConsumers_BothReceiveMessage()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "fan@example.com", 150.00m);
        await publisher.Publish(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(
            () => _orderTracker.Messages.Count >= 1 && _auditTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_orderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(_auditTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_orderTracker.Messages.Single().OrderId));
            Assert.That(msg.OrderId, Is.EqualTo(_auditTracker.Messages.Single().OrderId));
        }
    }
}
