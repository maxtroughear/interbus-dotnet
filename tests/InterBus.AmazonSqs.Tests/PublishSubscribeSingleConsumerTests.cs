using Amazon.Extensions.NETCore.Setup;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class PublishSubscribeSingleConsumerTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private OrderPlacedTracker _orderTracker = null!;
    private string _runId = null!;
    private string _queuePrefix = null!;
    private string _topicPrefix = null!;

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
        _runId = Guid.NewGuid().ToString("N")[..8]; // 8 hex chars for uniqueness
        _queuePrefix = $"q-{_runId}-";
        _topicPrefix = $"t-{_runId}-";
        _orderTracker = new OrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);

        // Register AWS clients pointing at the LocalStack container (or real AWS when
        // INTERBUS_USE_REAL_AWS=true). In production the app registers clients once
        // (e.g. via AddAWSService<T>()) and InterBus simply resolves them from DI.
        AWSOptions awsOptions = TestHelpers.CreateAwsOptions(Container);
        services.AddAWSService<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(awsOptions);
        services.AddAWSService<Amazon.SQS.IAmazonSQS>(awsOptions);
        services.AddAWSService<Amazon.SecurityToken.IAmazonSecurityTokenService>(awsOptions);

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                // Unique per-test prefix isolates queues and topics so tests sharing
                // the same container don't interfere with each other.
                o.QueuePrefix = _queuePrefix;
                o.TopicPrefix = _topicPrefix;
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

        // Clean up AWS resources created during this test
        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();
        var snsClient = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var logger = _provider.GetRequiredService<ILogger<PublishSubscribeSingleConsumerTests>>();

        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, _queuePrefix, _topicPrefix, logger);

        await _provider.DisposeAsync();
    }

    [Test]
    public async Task Publish_SingleConsumer_MessageIsDelivered()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

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
        await publisher.SendAsync<OrderPlacedConsumer, OrderPlaced>(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _orderTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_orderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_orderTracker.Messages.Single().OrderId));
        }
    }
}
