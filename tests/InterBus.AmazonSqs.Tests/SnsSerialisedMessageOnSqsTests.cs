using InterBus.Generated;
using System.Text.Json;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.AmazonSqs.Extensions;
using Microsoft.Extensions.DependencyInjection;
using SnsMessageAttributeValue = Amazon.SimpleNotificationService.Model.MessageAttributeValue;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public class SnsSerialisedMessageOnSqsTests() : LocalstackTestFixtureBase(bus =>
{
    bus.Services.AddSingleton(new SnsSerialisedTracker());
    bus.AddConsumer<SnsSerialisedOrderPlacedConsumer>(c => c.UseSnsSerialisedMessageOnSqs());
})
{
    private SnsSerialisedTracker Tracker => Provider.GetRequiredService<SnsSerialisedTracker>();

    [Test]
    public async Task Consume_SnsSerialisedMessageOnSqs_PullsHeadersFromSnsMessageAttributes()
    {
        var sns = Provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var sqs = Provider.GetRequiredService<IAmazonSQS>();
        var topologyBuilder = Provider.GetRequiredService<ITopologyBuilder>() as SnsSqsTopologyBuilder ??
                              throw new InvalidOperationException(
                                  "Expected ITopologyBuilder to be of type SnsSqsTopologyBuilder.");
        var cancellationToken = TestContext.CurrentContext.CancellationToken;

        var queueUrl = await topologyBuilder.EnsureQueueAsync(
            RoutingConventions.QueueUri(SqsTransport.QueueScheme, typeof(SnsSerialisedOrderPlacedConsumer)),
            cancellationToken);

        var queueArn = (await sqs.GetQueueAttributesAsync(
                           new GetQueueAttributesRequest { QueueUrl = queueUrl, AttributeNames = ["QueueArn"] },
                           cancellationToken)).QueueARN
                       ?? throw new InvalidOperationException($"Queue ARN not returned for queue URL '{queueUrl}'.");

        var topicArn = await topologyBuilder.EnsureTopicAsync(
            RoutingConventions.TopicUri(SnsTransport.TopicScheme, typeof(OrderPlaced)),
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

        await WaitUntilAsync(() => Tracker.Messages.Count >= 1);

        var consumed = Tracker.Messages.Single();
        using (Assert.EnterMultipleScope())
        {
            Assert.That(consumed.Message.OrderId, Is.EqualTo(order.OrderId));
            Assert.That(consumed.Headers["x-custom"], Is.EqualTo(customHeader));
            Assert.That(consumed.Headers[InterBusHeaders.MessageType], Is.EqualTo(messageType));
        }
    }
}
