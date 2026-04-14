using Amazon;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
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
        new LocalStackBuilder("localstack/localstack:3.8.1").Build();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private OrderPlacedTracker _orderTracker = null!;
    // Unique prefix per test-setup so that different tests don't share queues/topics.
    private string _runId = null!;

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
        _runId = Guid.NewGuid().ToString("N")[..8]; // 8 hex chars for uniqueness
        _orderTracker = new OrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);

        // Register AWS clients pointing at the LocalStack container.
        // In production the app registers clients once (e.g. via AddAWSService<T>()) and
        // InterBus simply resolves them from DI.
        var localStackOptions = new AWSOptions
        {
            Credentials = new BasicAWSCredentials("test", "test"),
            Region = RegionEndpoint.USEast1,
        };
        localStackOptions.DefaultClientConfig.ServiceURL = Container.GetConnectionString();
        services.AddAWSService<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(localStackOptions);
        services.AddAWSService<Amazon.SQS.IAmazonSQS>(localStackOptions);

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                // Unique per-test prefix isolates queues and topics so tests sharing
                // the same LocalStack container don't interfere with each other.
                o.QueuePrefix = $"q-{_runId}-";
                o.TopicPrefix = $"t-{_runId}-";
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
