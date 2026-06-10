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
public class FifoTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
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

    [TearDown]
    public async Task TearDown()
    {
        await _cts.CancelAsync();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StopAsync(CancellationToken.None);

        _cts.Dispose();

        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();
        var snsClient = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var logger = _provider.GetRequiredService<ILogger<FifoTests>>();

        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, _queuePrefix, _topicPrefix, logger);

        await _provider.DisposeAsync();
    }

    private async Task SetupProvider(Action<InterBusBuilder> configureBus)
    {
        _runId = Guid.NewGuid().ToString("N")[..8];
        _queuePrefix = $"q-{_runId}-";
        _topicPrefix = $"t-{_runId}-";

        var services = new ServiceCollection();
        services.AddLogging();

        var awsOptions = TestHelpers.CreateAwsOptions(Container);
        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);

        services.AddAWSService<IAmazonSimpleNotificationService>();
        services.AddAWSService<IAmazonSQS>();
        services.AddAWSService<Amazon.SecurityToken.IAmazonSecurityTokenService>();

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                o.QueuePrefix = _queuePrefix;
                o.TopicPrefix = _topicPrefix;
            });
            configureBus(bus);
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [Test]
    public async Task Publish_FifoTopic_MessageDeliveredToFifoQueue()
    {
        var tracker = new FifoOrderPlacedTracker();

        await SetupProvider(bus =>
        {
            bus.Configure<FifoOrderPlaced>(c => c.Fifo(o => o.CustomerId));
            bus.Services.AddSingleton(tracker);
            bus.AddConsumer<FifoOrderPlacedConsumer>(c => c.WithFifo());
        });

        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new FifoOrderPlaced(Guid.NewGuid(), "cust-1", 99.99m);
        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => tracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    [Test]
    public async Task Publish_FifoTopic_MultipleGroupsDeliveredInOrder()
    {
        var tracker = new FifoOrderPlacedTracker();

        await SetupProvider(bus =>
        {
            bus.Configure<FifoOrderPlaced>(c => c.Fifo(o => o.CustomerId));
            bus.Services.AddSingleton(tracker);
            bus.AddConsumer<FifoOrderPlacedConsumer>(c => c.WithFifo());
        });

        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var orders = Enumerable.Range(1, 5)
            .Select(i => new FifoOrderPlaced(Guid.NewGuid(), $"cust-{i}", i * 10m))
            .ToList();

        foreach (var order in orders)
            await publisher.PublishAsync(order, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => tracker.Messages.Count >= orders.Count);

        Assert.That(tracker.Messages, Has.Count.EqualTo(orders.Count));
        Assert.That(tracker.Messages.Select(m => m.OrderId),
            Is.EquivalentTo(orders.Select(o => o.OrderId)));
    }

    [Test]
    public async Task Send_FifoQueue_MessageDeliveredDirectly()
    {
        var tracker = new FifoOrderDirectTracker();

        await SetupProvider(bus =>
        {
            bus.Configure<FifoOrderDirect>(c => c.Fifo(o => o.CustomerId));
            bus.Services.AddSingleton(tracker);
            bus.AddConsumer<FifoOrderDirectConsumer>(c => c.WithFifo());
        });

        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new FifoOrderDirect(Guid.NewGuid(), "cust-42");
        await publisher.SendAsync<FifoOrderDirectConsumer, FifoOrderDirect>(
            msg, TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => tracker.Messages.Count >= 1);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(tracker.Messages, Has.Count.EqualTo(1));
            Assert.That(tracker.Messages.Single().OrderId, Is.EqualTo(msg.OrderId));
        }
    }

    [Test]
    public async Task Publish_FifoTopic_TopicAndQueueNameHaveFifoSuffix()
    {
        var tracker = new FifoOrderPlacedTracker();

        await SetupProvider(bus =>
        {
            bus.Configure<FifoOrderPlaced>(c => c.Fifo(o => o.CustomerId));
            bus.Services.AddSingleton(tracker);
            bus.AddConsumer<FifoOrderPlacedConsumer>(c => c.WithFifo());
        });

        var snsClient = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();

        // Trigger topology creation by publishing
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(
            new FifoOrderPlaced(Guid.NewGuid(), "cust-1", 1m),
            TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => tracker.Messages.Count >= 1);

        // Verify SNS topic has .fifo suffix
        var topics = await snsClient.ListTopicsAsync(TestContext.CurrentContext.CancellationToken);
        var fifoTopic = topics.Topics.FirstOrDefault(t =>
            t.TopicArn.Contains(_topicPrefix, StringComparison.Ordinal) &&
            t.TopicArn.EndsWith(".fifo", StringComparison.Ordinal));
        Assert.That(fifoTopic, Is.Not.Null, "Expected a .fifo SNS topic to be created");

        // Verify SQS queue has .fifo suffix
        var queues = await sqsClient.ListQueuesAsync(_queuePrefix, TestContext.CurrentContext.CancellationToken);
        var fifoQueue = queues.QueueUrls.FirstOrDefault(u =>
            u.EndsWith(".fifo", StringComparison.Ordinal));
        Assert.That(fifoQueue, Is.Not.Null, "Expected a .fifo SQS queue to be created");
    }
}
