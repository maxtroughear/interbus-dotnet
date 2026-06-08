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
using SnsMessageAttributeValue = Amazon.SimpleNotificationService.Model.MessageAttributeValue;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class SnsSerialisedMessageOnSqsTests
{
    private static readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();

    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private SnsSerialisedTracker _tracker = null!;
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
        _tracker = new SnsSerialisedTracker();

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
            });
            bus.AddConsumer<SnsSerialisedOrderPlacedConsumer>(c => c.UseSnsSerialisedMessageOnSqs());
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
        var logger = _provider.GetRequiredService<ILogger<SnsSerialisedMessageOnSqsTests>>();
        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, _queuePrefix, _topicPrefix, logger);
        await _provider.DisposeAsync();
    }

    [Test]
    public async Task Consume_SnsSerialisedMessageOnSqs_PullsHeadersFromSnsMessageAttributes()
    {
        var sns = _provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var sqs = _provider.GetRequiredService<IAmazonSQS>();
        var topologyBuilder = _provider.GetRequiredService<ITopologyBuilder>() as SnsSqsTopologyBuilder ??
                              throw new InvalidOperationException(
                                  "Expected ITopologyBuilder to be of type SnsSqsTopologyBuilder.");
        var cancellationToken = TestContext.CurrentContext.CancellationToken;

        var queueUrl = await topologyBuilder.EnsureQueueAsync(
            RoutingConventions.QueueName(typeof(SnsSerialisedOrderPlacedConsumer)),
            cancellationToken);

        var queueArn = (await sqs.GetQueueAttributesAsync(
                           new GetQueueAttributesRequest { QueueUrl = queueUrl, AttributeNames = ["QueueArn"] },
                           cancellationToken)).QueueARN
                       ?? throw new InvalidOperationException($"Queue ARN not returned for queue URL '{queueUrl}'.");

        var topicArn = await topologyBuilder.EnsureTopicAsync(
            RoutingConventions.TopicName(typeof(OrderPlaced)),
            cancellationToken);

        var policy = JsonSerializer.Serialize(new
        {
            Version = "2012-10-17",
            Statement = new[]
            {
                new
                {
                    Effect = "Allow",
                    Principal = new { Service = "sns.amazonaws.com" },
                    Action = "sqs:SendMessage",
                    Resource = queueArn,
                    Condition = new
                    {
                        StringEquals = new Dictionary<string, string>
                        {
                            ["aws:SourceArn"] = topicArn
                        }
                    }
                }
            }
        });

        await sqs.SetQueueAttributesAsync(
            new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string> { ["Policy"] = policy },
            },
            cancellationToken);

        static string? FindConfirmedSubscriptionArn(
            List<Subscription>? subscriptions,
            string queueArnValue) =>
            (subscriptions ?? [])
            .FirstOrDefault(s =>
                string.Equals(s.Endpoint, queueArnValue, StringComparison.Ordinal)
                && !string.Equals(s.SubscriptionArn, "PendingConfirmation", StringComparison.Ordinal))
            ?.SubscriptionArn;

        var subscriptions = await sns.ListSubscriptionsByTopicAsync(
            new ListSubscriptionsByTopicRequest { TopicArn = topicArn },
            cancellationToken);

        var subscriptionArn = FindConfirmedSubscriptionArn(subscriptions.Subscriptions, queueArn);

        if (string.IsNullOrWhiteSpace(subscriptionArn))
        {
            subscriptionArn = (await sns.SubscribeAsync(
                new SubscribeRequest
                {
                    TopicArn = topicArn,
                    Protocol = "sqs",
                    Endpoint = queueArn,
                    Attributes = new Dictionary<string, string> { ["RawMessageDelivery"] = "false" },
                },
                cancellationToken)).SubscriptionArn;
        }

        if (string.Equals(subscriptionArn, "PendingConfirmation", StringComparison.Ordinal))
        {
            var refreshed = await sns.ListSubscriptionsByTopicAsync(
                new ListSubscriptionsByTopicRequest { TopicArn = topicArn },
                cancellationToken);

            subscriptionArn = FindConfirmedSubscriptionArn(refreshed.Subscriptions, queueArn);
        }

        Assert.That(subscriptionArn, Is.Not.Null.And.Not.EqualTo("PendingConfirmation"));

        await sns.SetSubscriptionAttributesAsync(
            new SetSubscriptionAttributesRequest
            {
                SubscriptionArn = subscriptionArn!,
                AttributeName = "RawMessageDelivery",
                AttributeValue = "false",
            },
            cancellationToken);

        var order = new OrderPlaced(Guid.NewGuid(), "interop@example.com", 5.20m);
        var payload = JsonSerializer.Serialize(order);
        var messageType = typeof(OrderPlaced).FullName!;
        const string customHeader = "custom-value";

        await sns.PublishAsync(
            new PublishRequest
            {
                TopicArn = topicArn,
                Message = payload,
                MessageAttributes = new Dictionary<string, SnsMessageAttributeValue>
                {
                    [InterBusHeaders.MessageType] = new() { DataType = "String", StringValue = messageType },
                    [InterBusHeaders.MessageId] = new()
                        { DataType = "String", StringValue = Guid.NewGuid().ToString("N") },
                    ["x-custom"] = new() { DataType = "String", StringValue = customHeader },
                },
            },
            TestContext.CurrentContext.CancellationToken);

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= 1);

        var consumed = _tracker.Messages.Single();
        using (Assert.EnterMultipleScope())
        {
            Assert.That(consumed.Message.OrderId, Is.EqualTo(order.OrderId));
            Assert.That(consumed.Headers["x-custom"], Is.EqualTo(customHeader));
            Assert.That(consumed.Headers[InterBusHeaders.MessageType], Is.EqualTo(messageType));
        }
    }
}

public sealed class SnsSerialisedTracker
{
    private readonly ConcurrentBag<SnsSerialisedConsumedMessage> _messages = [];
    public IReadOnlyCollection<SnsSerialisedConsumedMessage> Messages => _messages;

    public void Track(OrderPlaced message, MessageHeaders headers) =>
        _messages.Add(new SnsSerialisedConsumedMessage(message, headers));
}

public sealed record SnsSerialisedConsumedMessage(OrderPlaced Message, MessageHeaders Headers);

public sealed class SnsSerialisedOrderPlacedConsumer(SnsSerialisedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message, new MessageHeaders(context.Headers));
        return Task.CompletedTask;
    }
}
