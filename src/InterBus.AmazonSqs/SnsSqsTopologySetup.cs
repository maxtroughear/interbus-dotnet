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
    /// In <see cref="TopologyMode.VerifyOnly"/> mode only the queue URL is resolved.
    /// Topic verification is deferred until the first publish via
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
    /// it is not already cached. In <see cref="TopologyMode.VerifyOnly"/> mode the topic is
    /// located by paginating <c>ListTopicsAsync</c> — no create or write operations are issued.
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
    /// Reads the existing SQS queue URL for every registered consumer and populates the
    /// <see cref="SnsSqsClientProvider"/> cache. Does not verify SNS topics — that is
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

            await VerifyQueueUrlAsync(queueName, cancellationToken);
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
    /// Resolves the ARN for an existing SNS topic by paginating <c>ListTopicsAsync</c> until
    /// a topic whose ARN ends with <c>:{topicName}</c> is found.
    /// Read-only — does not create the topic.
    /// </summary>
    private async Task<string> VerifyTopicArnAsync(string topicName, CancellationToken cancellationToken)
    {
        logger.LogDebug("InterBus AmazonSqs: looking up SNS topic {TopicName}", topicName);

        string? nextToken = null;
        do
        {
            var response = await snsClient.ListTopicsAsync(
                new ListTopicsRequest { NextToken = nextToken },
                cancellationToken);

            var match = response.Topics.Find(
                t => t.TopicArn[(t.TopicArn.LastIndexOf(':') + 1)..] == topicName);

            if (match is not null)
            {
                clientProvider.TopicArns.TryAdd(topicName, match.TopicArn);
                return match.TopicArn;
            }

            nextToken = response.NextToken;
        }
        while (nextToken is not null);

        throw new InvalidOperationException(
            $"SNS topic '{topicName}' was not found. " +
            $"Ensure the topic has been created before starting the application in " +
            $"{nameof(TopologyMode.VerifyOnly)} mode.");
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
