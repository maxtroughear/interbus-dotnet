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
/// <para>
/// When <see cref="SnsSqsTransportOptions.TopologyMode"/> is
/// <see cref="TopologyMode.VerifyOnly"/> the setup only reads the existing resources to
/// populate internal caches without attempting any create, subscribe, or policy operations.
/// This suits applications that run under an IAM role without infrastructure-management
/// permissions, where the topology is provisioned out-of-band (Terraform, CloudFormation,
/// AWS CDK, etc.) or via a dedicated initialization step.
/// </para>
/// </summary>
internal sealed class SnsSqsTopologySetup(
    IAmazonSimpleNotificationService snsClient,
    IAmazonSQS sqsClient,
    SnsSqsClientProvider clientProvider,
    SnsSqsTransportOptions transportOptions,
    InterBusOptions busOptions,
    ILogger<SnsSqsTopologySetup> logger)
{
    /// <summary>
    /// Declares (or verifies) the topology for every registered consumer, then populates
    /// the <see cref="SnsSqsClientProvider"/> caches with queue URLs and topic ARNs.
    /// </summary>
    internal async Task DeclareTopologyAsync(CancellationToken cancellationToken)
    {
        if (transportOptions.TopologyMode == TopologyMode.VerifyOnly)
        {
            await VerifyTopologyAsync(cancellationToken);
        }
        else
        {
            await CreateOrVerifyTopologyAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Creates SQS queues, SNS topics, subscriptions, and queue resource policies for every
    /// registered consumer. Idempotent: already-existing resources are silently accepted.
    /// </summary>
    private async Task CreateOrVerifyTopologyAsync(CancellationToken cancellationToken)
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
    /// Reads the existing SNS/SQS topology and populates the client-provider caches without
    /// attempting any create, subscribe, or policy operations. Used when
    /// <see cref="SnsSqsTransportOptions.TopologyMode"/> is
    /// <see cref="TopologyMode.VerifyOnly"/>.
    /// <para>
    /// SNS topic ARNs are derived from the SQS queue ARN (which encodes the AWS partition,
    /// region, and account ID), avoiding an additional STS call.
    /// </para>
    /// </summary>
    private async Task VerifyTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = clientProvider.BuildSqsQueueName(descriptor.QueueName);

            logger.LogDebug(
                "InterBus AmazonSqs: verifying queue {QueueName} for consumer {ConsumerType}",
                queueName,
                descriptor.ConsumerType.Name);

            // Read-only: get the URL of the pre-existing queue.
            var queueUrlResponse = await sqsClient.GetQueueUrlAsync(
                new GetQueueUrlRequest { QueueName = queueName },
                cancellationToken);

            var queueUrl = queueUrlResponse.QueueUrl;
            clientProvider.QueueUrls.TryAdd(queueName, queueUrl);

            // Retrieve the queue ARN so we can derive the SNS topic ARNs for the same
            // partition / region / account without an STS call.
            var attrs = await sqsClient.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = ["QueueArn"],
                },
                cancellationToken);

            var queueArn = attrs.QueueARN;

            // Queue ARN format: arn:{partition}:sqs:{region}:{account-id}:{name}
            // We extract components to build SNS topic ARNs (same partition/region/account).
            var arnParts = queueArn.Split(':');
            if (arnParts.Length < 6)
            {
                throw new InvalidOperationException(
                    $"Unexpected SQS queue ARN format '{queueArn}': " +
                    "expected 'arn:{partition}:sqs:{region}:{account-id}:{name}'.");
            }

            foreach (var messageType in descriptor.MessageTypes)
            {
                var rawTopicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var topicName = clientProvider.BuildSnsTopicName(rawTopicName);

                // Construct the topic ARN from queue-ARN components instead of an STS call:
                //   arn:{partition}:sns:{region}:{account-id}:{topic-name}
                var topicArn = $"arn:{arnParts[1]}:sns:{arnParts[3]}:{arnParts[4]}:{topicName}";

                logger.LogDebug(
                    "InterBus AmazonSqs: verifying SNS topic {TopicName} (arn={TopicArn})",
                    topicName,
                    topicArn);

                // Confirm the topic exists (throws NotFoundException if it doesn't).
                await snsClient.GetTopicAttributesAsync(
                    new GetTopicAttributesRequest { TopicArn = topicArn },
                    cancellationToken);

                clientProvider.TopicArns.TryAdd(topicName, topicArn);
            }
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
