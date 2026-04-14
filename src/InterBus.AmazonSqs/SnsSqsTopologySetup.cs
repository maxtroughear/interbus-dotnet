using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

/// <summary>
/// Responsible for declaring the SNS/SQS topology required by registered InterBus consumers:
/// one SQS queue per consumer, one SNS topic per handled message type, and raw-delivery
/// subscriptions binding each queue to its topics.
/// <para>
/// Separating topology setup from the message pump means the pump only needs to focus on
/// polling, dispatching, and acknowledgement.
/// </para>
/// </summary>
internal sealed class SnsSqsTopologySetup(
    IAmazonSimpleNotificationService snsClient,
    IAmazonSQS sqsClient,
    SnsSqsClientProvider clientProvider,
    InterBusOptions busOptions,
    ILogger<SnsSqsTopologySetup> logger)
{
    /// <summary>
    /// Creates SQS queues, SNS topics, subscriptions, and queue resource policies for every
    /// registered consumer. Idempotent: already-existing resources are silently accepted.
    /// </summary>
    internal async Task DeclareTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = clientProvider.BuildSqsQueueName(descriptor.QueueName);

            logger.LogDebug(
                "InterBus AmazonSqs: declaring queue {QueueName} for consumer {ConsumerType}",
                queueName,
                descriptor.ConsumerType.Name);

            var queueResponse = await sqsClient.CreateQueueAsync(
                new CreateQueueRequest
                {
                    QueueName = queueName,
                    Attributes = new Dictionary<string, string>
                    {
                        ["MessageRetentionPeriod"] = "1209600", // 14 days
                    },
                },
                cancellationToken);

            var queueUrl = queueResponse.QueueUrl;
            clientProvider.QueueUrls.TryAdd(queueName, queueUrl);

            // Retrieve the queue ARN needed for the SQS resource policy.
            var attrs = await sqsClient.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = ["QueueArn"],
                },
                cancellationToken);

            var queueArn = attrs.QueueARN;

            // Subscribe the queue to each SNS topic the consumer handles.
            foreach (var messageType in descriptor.MessageTypes)
            {
                var rawTopicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var topicName = clientProvider.BuildSnsTopicName(rawTopicName);

                logger.LogDebug(
                    "InterBus AmazonSqs: declaring SNS topic {TopicName}",
                    topicName);

                // CreateTopicAsync is idempotent on SNS: if a topic with this name already
                // exists (including when called concurrently), the call succeeds and returns
                // the existing topic ARN — no duplicate topics are created.
                var topicResponse = await snsClient.CreateTopicAsync(
                    topicName,
                    cancellationToken);

                var topicArn = topicResponse.TopicArn;
                clientProvider.TopicArns.TryAdd(topicName, topicArn);

                // Subscribe queue to topic.
                var subscribeResponse = await snsClient.SubscribeAsync(
                    new SubscribeRequest
                    {
                        TopicArn = topicArn,
                        Protocol = "sqs",
                        Endpoint = queueArn,
                        Attributes = new Dictionary<string, string>
                        {
                            // Raw message delivery passes the original payload and
                            // message attributes directly to SQS without wrapping in
                            // an SNS notification JSON envelope.
                            ["RawMessageDelivery"] = "true",
                        },
                    },
                    cancellationToken);

                logger.LogDebug(
                    "InterBus AmazonSqs: subscribed queue {QueueName} to topic {TopicName} (subscriptionArn={SubscriptionArn})",
                    queueName,
                    topicName,
                    subscribeResponse.SubscriptionArn);
            }

            // Allow SNS to send messages to this SQS queue.
            await SetQueuePolicyAsync(queueUrl, queueArn, descriptor, cancellationToken);
        }
    }

    /// <summary>
    /// Attaches an SQS resource-based policy that allows all SNS topics associated with
    /// this consumer's message types to send messages to the queue.
    /// </summary>
    private async Task SetQueuePolicyAsync(
        string queueUrl,
        string queueArn,
        ConsumerDescriptor descriptor,
        CancellationToken cancellationToken)
    {
        var topicArns = descriptor.MessageTypes.Select(t =>
        {
            var rawName = busOptions.TryGetTopicName(t, out var custom)
                ? custom
                : RoutingConventions.TopicName(t);

            return clientProvider.TopicArns.GetValueOrDefault(
                clientProvider.BuildSnsTopicName(rawName));
        }).OfType<string>().Distinct().ToList();

        if (topicArns.Count == 0)
            return;

        var arnList = topicArns.Count == 1
            ? $"\"{topicArns[0]}\""
            : "[" + string.Join(",", topicArns.Select(a => $"\"{a}\"")) + "]";

        var conditionBlock =
            "{\"StringEquals\":{\"aws:SourceArn\":" + arnList + "}}";

        var policy =
            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\"," +
            "\"Principal\":{\"Service\":\"sns.amazonaws.com\"}," +
            "\"Action\":\"sqs:SendMessage\"," +
            $"\"Resource\":\"{queueArn}\"," +
            $"\"Condition\":{conditionBlock}" +
            "}]}";

        await sqsClient.SetQueueAttributesAsync(
            new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string> { ["Policy"] = policy },
            },
            cancellationToken);
    }
}
