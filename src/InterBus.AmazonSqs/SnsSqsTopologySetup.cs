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
/// The topology is split by concern:
/// <list type="bullet">
/// <item><term>Consumer topology</term><description>Handled by
/// <see cref="DeclareConsumerTopologyAsync"/>. Establishes the SQS queues needed for
/// polling. In <see cref="TopologyMode.VerifyOnly"/> mode only the queue URL is resolved;
/// no topic or subscription operations are performed.</description></item>
/// <item><term>Publisher topology</term><description>Handled lazily by
/// <see cref="EnsureTopicArnAsync"/> (SNS fan-out) and
/// <see cref="EnsureQueueUrlAsync"/> (SQS direct-send). Only the ARN or URL required for
/// sending is resolved.</description></item>
/// </list>
/// </para>
/// <para>
/// When <see cref="SnsSqsTransportOptions.TopologyMode"/> is
/// <see cref="TopologyMode.VerifyOnly"/> all operations are read-only: no create, subscribe,
/// or policy writes are issued. This suits applications running under a least-privilege IAM
/// role where infrastructure is provisioned out-of-band (Terraform, CloudFormation, CDK) or
/// via a one-time initialization step.
/// </para>
/// </summary>
public sealed class SnsSqsTopologySetup(
    IAmazonSimpleNotificationService snsClient,
    IAmazonSQS sqsClient,
    SnsSqsClientProvider clientProvider,
    SnsSqsTransportOptions transportOptions,
    InterBusOptions busOptions,
    ILogger<SnsSqsTopologySetup> logger)
{
    // -------------------------------------------------------------------------
    // Consumer topology (called by SqsMessagePump at startup)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Declares (or verifies) the consumer topology for every registered consumer, populating
    /// the <see cref="SnsSqsClientProvider"/> caches.
    /// <para>
    /// In <see cref="TopologyMode.CreateOrVerify"/> mode this creates SQS queues, SNS topics,
    /// raw-delivery subscriptions, and queue resource policies (idempotent).
    /// </para>
    /// <para>
    /// In <see cref="TopologyMode.VerifyOnly"/> mode only the queue URL and queue ARN are
    /// resolved. Topic verification is deferred until the first publish via
    /// <see cref="EnsureTopicArnAsync"/>.
    /// </para>
    /// </summary>
    internal async Task DeclareConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        if (transportOptions.TopologyMode == TopologyMode.VerifyOnly)
            await VerifyConsumerTopologyAsync(cancellationToken);
        else
            await CreateOrVerifyConsumerTopologyAsync(cancellationToken);
    }

    // -------------------------------------------------------------------------
    // Publisher / sender topology (called lazily by SnsTransport / SqsTransport)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Ensures the SNS topic for <paramref name="rawTopicName"/> exists and returns its ARN.
    /// <para>
    /// In <see cref="TopologyMode.CreateOrVerify"/> mode the topic is created (idempotent) if
    /// it is not already cached. In <see cref="TopologyMode.VerifyOnly"/> mode the ARN is
    /// derived from a cached SQS queue ARN (same partition/region/account) and the topic's
    /// existence is confirmed via <c>GetTopicAttributesAsync</c>.
    /// </para>
    /// </summary>
    internal async Task<string> EnsureTopicArnAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        var topicName = clientProvider.BuildSnsTopicName(rawTopicName);

        if (clientProvider.TopicArns.TryGetValue(topicName, out var cachedArn))
            return cachedArn;

        return transportOptions.TopologyMode == TopologyMode.VerifyOnly
            ? await VerifyTopicArnAsync(topicName, cancellationToken)
            : await CreateTopicAsync(topicName, cancellationToken);
    }

    /// <summary>
    /// Ensures the SQS queue for <paramref name="rawQueueName"/> exists and returns its URL.
    /// <para>
    /// In <see cref="TopologyMode.CreateOrVerify"/> mode the queue is created (idempotent) if
    /// it is not already cached. In <see cref="TopologyMode.VerifyOnly"/> mode the URL is
    /// resolved via <c>GetQueueUrlAsync</c>.
    /// </para>
    /// </summary>
    internal async Task<string> EnsureQueueUrlAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        var queueName = clientProvider.BuildSqsQueueName(rawQueueName);

        if (clientProvider.QueueUrls.TryGetValue(queueName, out var cachedUrl))
            return cachedUrl;

        return transportOptions.TopologyMode == TopologyMode.VerifyOnly
            ? await VerifyQueueUrlAsync(queueName, cancellationToken)
            : await CreateQueueAsync(queueName, cancellationToken);
    }

    // -------------------------------------------------------------------------
    // CreateOrVerify private helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Creates SQS queues, SNS topics, subscriptions, and queue resource policies for every
    /// registered consumer. Idempotent: already-existing resources are silently accepted.
    /// </summary>
    private async Task CreateOrVerifyConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = clientProvider.BuildSqsQueueName(descriptor.QueueName);

            logger.LogDebug(
                "InterBus AmazonSqs: declaring queue {QueueName} for consumer {ConsumerType}",
                queueName,
                descriptor.ConsumerType.Name);

            var queueUrl = await CreateQueueAsync(queueName, cancellationToken);

            // Retrieve the queue ARN needed for the SQS resource policy.
            var attrs = await sqsClient.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = ["QueueArn"],
                },
                cancellationToken);

            var queueArn = attrs.QueueARN;
            clientProvider.QueueArns.TryAdd(queueName, queueArn);

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

                var topicArn = await CreateTopicAsync(topicName, cancellationToken);

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
    /// Creates an SQS queue with the configured retention period and (optionally) SSE-SQS
    /// encryption. Returns the queue URL. Idempotent via SQS <c>CreateQueue</c> semantics.
    /// </summary>
    private async Task<string> CreateQueueAsync(string queueName, CancellationToken cancellationToken)
    {
        var attributes = new Dictionary<string, string>
        {
            ["MessageRetentionPeriod"] = "1209600", // 14 days
        };

        if (transportOptions.EnableSqsEncryption)
        {
            // SSE-SQS: SQS-managed encryption. No KMS charges; no cross-service key policy
            // required. The KmsMasterKeyId attribute is intentionally left unset so that
            // SQS uses its own managed key rather than a customer KMS key.
            attributes["SqsManagedSseEnabled"] = "true";
        }

        var response = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest { QueueName = queueName, Attributes = attributes },
            cancellationToken);

        var url = response.QueueUrl;
        clientProvider.QueueUrls.TryAdd(queueName, url);
        return url;
    }

    /// <summary>
    /// Creates an SNS topic with (optionally) the AWS managed key for SNS encryption.
    /// Returns the topic ARN. Idempotent via SNS <c>CreateTopic</c> semantics.
    /// </summary>
    private async Task<string> CreateTopicAsync(string topicName, CancellationToken cancellationToken)
    {
        var topicAttributes = new Dictionary<string, string>();

        if (transportOptions.EnableSnsEncryption)
        {
            // AWS managed key for SNS (alias/aws/sns). No customer-managed KMS key required.
            topicAttributes["KmsMasterKeyId"] = "alias/aws/sns";
        }

        // CreateTopicAsync is idempotent on SNS: if a topic with this name already exists
        // (including when called concurrently), the call succeeds and returns the existing ARN.
        var response = await snsClient.CreateTopicAsync(
            new CreateTopicRequest { Name = topicName, Attributes = topicAttributes },
            cancellationToken);

        var arn = response.TopicArn;
        clientProvider.TopicArns.TryAdd(topicName, arn);
        return arn;
    }

    // -------------------------------------------------------------------------
    // VerifyOnly private helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Reads the existing SQS queue URL and ARN for every registered consumer and populates
    /// the <see cref="SnsSqsClientProvider"/> caches. Does not verify SNS topics — that is
    /// deferred to the first publish via <see cref="EnsureTopicArnAsync"/>.
    /// </summary>
    private async Task VerifyConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = clientProvider.BuildSqsQueueName(descriptor.QueueName);

            logger.LogDebug(
                "InterBus AmazonSqs: verifying queue {QueueName} for consumer {ConsumerType}",
                queueName,
                descriptor.ConsumerType.Name);

            var queueUrl = await VerifyQueueUrlAsync(queueName, cancellationToken);

            // Retrieve and cache the queue ARN so that EnsureTopicArnAsync can derive SNS
            // topic ARNs (same partition/region/account) without an STS call.
            var attrs = await sqsClient.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = ["QueueArn"],
                },
                cancellationToken);

            clientProvider.QueueArns.TryAdd(queueName, attrs.QueueARN);
        }
    }

    /// <summary>
    /// Resolves the URL for an existing SQS queue. Read-only — does not create the queue.
    /// </summary>
    private async Task<string> VerifyQueueUrlAsync(string queueName, CancellationToken cancellationToken)
    {
        var response = await sqsClient.GetQueueUrlAsync(
            new GetQueueUrlRequest { QueueName = queueName },
            cancellationToken);

        var url = response.QueueUrl;
        clientProvider.QueueUrls.TryAdd(queueName, url);
        return url;
    }

    /// <summary>
    /// Resolves the ARN for an existing SNS topic by deriving the ARN from any cached SQS
    /// queue ARN (same partition/region/account) and confirming existence via
    /// <c>GetTopicAttributesAsync</c>. Read-only — does not create the topic.
    /// </summary>
    private async Task<string> VerifyTopicArnAsync(string topicName, CancellationToken cancellationToken)
    {
        // Derive partition/region/account from any cached queue ARN to avoid an STS call.
        // Queue ARN format: arn:{partition}:sqs:{region}:{account-id}:{name}
        var queueArn = clientProvider.QueueArns.Values.FirstOrDefault()
            ?? throw new InvalidOperationException(
                $"Cannot derive SNS topic ARN for '{topicName}' in {nameof(TopologyMode.VerifyOnly)} mode: " +
                "no SQS queue ARNs are cached. Register at least one consumer so that the " +
                $"message pump populates the queue ARN cache before the first publish, or " +
                $"switch to {nameof(TopologyMode.CreateOrVerify)} topology mode.");

        var arnParts = queueArn.Split(':');
        if (arnParts.Length < 6)
        {
            throw new InvalidOperationException(
                $"Unexpected SQS queue ARN format '{queueArn}': " +
                "expected 'arn:{partition}:sqs:{region}:{account-id}:{name}'.");
        }

        // Construct: arn:{partition}:sns:{region}:{account-id}:{topic-name}
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
        return topicArn;
    }

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

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
