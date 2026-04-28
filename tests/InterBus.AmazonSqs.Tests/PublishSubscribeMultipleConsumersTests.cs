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
public class PublishSubscribeMultipleConsumersTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private OrderPlacedTracker _orderTracker = null!;
    private AuditTracker _auditTracker = null!;
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
        _runId = Guid.NewGuid().ToString("N")[..8];
        _queuePrefix = $"q-{_runId}-";
        _topicPrefix = $"t-{_runId}-";
        _orderTracker = new OrderPlacedTracker();
        _auditTracker = new AuditTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_orderTracker);
        services.AddSingleton(_auditTracker);

        AWSOptions awsOptions = TestHelpers.CreateAwsOptions(Container);
        services.AddAWSService<IAmazonSimpleNotificationService>(awsOptions);
        services.AddAWSService<IAmazonSQS>(awsOptions);
        services.AddAWSService<Amazon.SecurityToken.IAmazonSecurityTokenService>(awsOptions);

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                o.QueuePrefix = _queuePrefix;
                o.TopicPrefix = _topicPrefix;
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

        // Clean up AWS resources created during this test
        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();
        var snsClient = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var logger = _provider.GetRequiredService<ILogger<PublishSubscribeMultipleConsumersTests>>();

        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, _queuePrefix, _topicPrefix, logger);

        await _provider.DisposeAsync();
    }

    [Test]
    public async Task Publish_MultipleConsumers_BothReceiveMessage()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var msg = new OrderPlaced(Guid.NewGuid(), "fan@example.com", 150.00m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _orderTracker.Messages.Count >= 1 && _auditTracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_orderTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(_auditTracker.Messages, Has.Count.EqualTo(1));
            Assert.That(msg.OrderId, Is.EqualTo(_orderTracker.Messages.Single().OrderId));
            Assert.That(msg.OrderId, Is.EqualTo(_auditTracker.Messages.Single().OrderId));
        }
    }
}
