using System.Collections.Concurrent;
using System.Text.Json;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

public sealed class SubscriptionFilteringTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private FilteredOrderPlacedTracker _tracker = null!;
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
        _tracker = new FilteredOrderPlacedTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_tracker);

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
                o.EnableSnsMessageBodyFiltering = true;
            });

            bus.AddConsumer<FilteredOrderPlacedConsumer>(c =>
            {
                c.FilterOnSubscription<NestedOrderPlaced>(order =>
                    order.Order.Email == "match@example.com" &&
                    (order.Value == "test" || order.Value == "other" || order.Value == "other2"));

                c.FilterOnSubscription<OrderPlaced>(headers => headers["order-value"] == "test");
            });
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

        var sqsClient = _provider.GetRequiredService<IAmazonSQS>();
        var snsClient = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var logger = _provider.GetRequiredService<ILogger<SubscriptionFilteringTests>>();
        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, _queuePrefix, _topicPrefix, logger);
        await _provider.DisposeAsync();
    }

    [Test]
    public async Task Consume_Filtering_OnSubscription()
    {
        using var scope = _provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        await publisher.PublishAsync(
            new NestedOrderPlaced("other", new OrderPlaced(Guid.NewGuid(), "skip@example.com", 5.20m)),
            TestContext.CurrentContext.CancellationToken);
        await publisher.PublishAsync(
            new NestedOrderPlaced("test", new OrderPlaced(Guid.NewGuid(), "match@example.com", 9.90m)),
            TestContext.CurrentContext.CancellationToken);
        await publisher.PublishAsync(
            new NestedOrderPlaced("other", new OrderPlaced(Guid.NewGuid(), "match@example.com", 3.10m)),
            TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= 2);
        await Task.Delay(500, TestContext.CurrentContext.CancellationToken);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_tracker.Messages, Has.Count.EqualTo(2));
            Assert.That(_tracker.Messages.All(x => x.Email == "match@example.com"), Is.True);
        }
    }

    [Test]
    public async Task Topology_Filter_TranslatesToPolicy()
    {
        var sns = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var sqs = _provider.GetRequiredService<IAmazonSQS>();
        var topologyBuilder = _provider.GetRequiredService<ITopologyBuilder>() as SnsSqsTopologyBuilder ??
                              throw new InvalidOperationException(
                                  "Expected ITopologyBuilder to be of type SnsSqsTopologyBuilder.");
        var cancellationToken = TestContext.CurrentContext.CancellationToken;

        var queueUrl = await topologyBuilder.EnsureQueueAsync(
            RoutingConventions.QueueUri(SqsTransport.QueueScheme, typeof(FilteredOrderPlacedConsumer)),
            cancellationToken);

        var queueArn = (await sqs.GetQueueAttributesAsync(
                           new GetQueueAttributesRequest { QueueUrl = queueUrl, AttributeNames = ["QueueArn"] },
                           cancellationToken)).QueueARN
                       ?? throw new InvalidOperationException($"Queue ARN not returned for queue URL '{queueUrl}'.");

        var topicArn = await topologyBuilder.EnsureTopicAsync(
            RoutingConventions.TopicUri(SnsTransport.TopicScheme, typeof(NestedOrderPlaced)),
            cancellationToken);

        var subscriptions = await sns.ListSubscriptionsByTopicAsync(
            new ListSubscriptionsByTopicRequest { TopicArn = topicArn },
            cancellationToken);

        var subscriptionArn = subscriptions.Subscriptions
            .Single(s => string.Equals(s.Endpoint, queueArn, StringComparison.Ordinal))
            .SubscriptionArn;

        var attributes = await sns.GetSubscriptionAttributesAsync(
            new GetSubscriptionAttributesRequest { SubscriptionArn = subscriptionArn },
            cancellationToken);

        var expectedBodyPolicy = JsonSerializer.Serialize(new Dictionary<string, string[]>
        {
            ["Order.Email"] = ["match@example.com"],
            ["Value"] = ["test", "other", "other2"]
        });

        using (Assert.EnterMultipleScope())
        {
            Assert.That(attributes.Attributes["FilterPolicyScope"], Is.EqualTo("MessageBody"));
            Assert.That(attributes.Attributes["FilterPolicy"], Is.EqualTo(expectedBodyPolicy));
        }
    }
}

public sealed class FilteredOrderPlacedTracker
{
    private readonly ConcurrentBag<OrderPlaced> _messages = [];
    public IReadOnlyCollection<OrderPlaced> Messages => _messages;

    public void Track(OrderPlaced message) => _messages.Add(message);
}

public sealed class FilteredOrderPlacedConsumer(FilteredOrderPlacedTracker tracker) : IConsumer<NestedOrderPlaced>
{
    public Task Consume(NestedOrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message.Order);
        return Task.CompletedTask;
    }
}
