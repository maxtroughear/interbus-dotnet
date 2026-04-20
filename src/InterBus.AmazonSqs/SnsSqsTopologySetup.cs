using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consumer;
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
/// <see cref="BuildTopologyAsync"/>. Establishes the SQS queues needed for
/// polling. In <see cref="TopologyMode.VerifyOnly"/> mode only the queue URL is resolved;
/// no topic or subscription operations are performed.</description></item>
/// <item><term>Publisher topology</term><description>Handled lazily by
/// <see cref="EnsureTopicAsync"/> (SNS fan-out) and
/// <see cref="EnsureQueueAsync"/> (SQS direct-send). Only the ARN or URL required for
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
public sealed partial class SnsSqsTopologySetup(
    IAmazonSimpleNotificationService snsClient,
    IAmazonSQS sqsClient,
    IAmazonSecurityTokenService stsClient,
    SnsSqsTopology topology,
    SnsSqsTransportOptions transportOptions,
    InterBusOptions busOptions,
    ILogger<SnsSqsTopologySetup> logger)
    : IInterBusTopologyBuilder
{
    private GetCallerIdentityResponse? _callerIdentity;

    /// <summary>
    /// Declares (or verifies) the consumer topology for every registered consumer, populating the
    /// <see cref="SnsSqsTopology"/> caches.
    /// <para>
    /// In <see cref="TopologyMode.CreateOrVerify"/> mode this creates SQS queues, SNS topics, raw-delivery
    /// subscriptions, and queue resource policies (idempotent).
    /// </para>
    /// <para>
    /// In <see cref="TopologyMode.VerifyOnly"/> mode only the queue URL is resolved. Topic verification is deferred
    /// until the first publish via <see cref="EnsureTopicAsync"/>.
    /// </para>
    /// </summary>
    public async Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        if (transportOptions.TopologyMode == TopologyMode.VerifyOnly)
            await VerifyConsumerTopologyAsync(cancellationToken);
        else
            await CreateConsumerTopologyAsync(cancellationToken);
    }

    public async Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        var topicName = topology.BuildSnsTopicName(rawTopicName);

        if (topology.TopicArns.TryGetValue(topicName, out var cachedArn))
            return cachedArn;

        return transportOptions.TopologyMode == TopologyMode.VerifyOnly
            ? await VerifyTopicAsync(topicName, cancellationToken)
            : await CreateTopicAsync(topicName, cancellationToken);
    }

    public async Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        var queueName = topology.BuildSqsQueueName(rawQueueName);

        if (topology.QueueUrls.TryGetValue(queueName, out var cachedUrl))
            return cachedUrl;

        return transportOptions.TopologyMode == TopologyMode.VerifyOnly
            ? await VerifyQueueAsync(queueName, cancellationToken)
            : await CreateQueueAsync(queueName, cancellationToken);
    }

    private async Task CreateConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = topology.BuildSqsQueueName(descriptor.QueueName);

            var queueUrl = await CreateQueueAsync(queueName, cancellationToken);

            logger.LogDebug(
                "InterBus AmazonSqs: declared queue {QueueName} for consumer {ConsumerType}, URL {QueueUrl}",
                queueName,
                descriptor.ConsumerType.Name,
                queueUrl);

            var attrs = await sqsClient.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = ["QueueArn"],
                },
                cancellationToken);

            var queueArn = attrs.QueueARN;

            foreach (var messageType in descriptor.MessageTypes)
            {
                var rawTopicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var topicName = topology.BuildSnsTopicName(rawTopicName);

                var topicArn = await CreateTopicAsync(topicName, cancellationToken);

                logger.LogDebug(
                    "InterBus AmazonSqs: declared SNS topic {TopicName}, ARN {TopicArn}",
                    topicName,
                    topicArn);

                var subscribeResponse = await snsClient.SubscribeAsync(
                    new SubscribeRequest
                    {
                        TopicArn = topicArn,
                        Protocol = "sqs",
                        Endpoint = queueArn,
                        Attributes = new Dictionary<string, string>
                        {
                            ["RawMessageDelivery"] = "true",
                        }
                    },
                    cancellationToken);

                logger.LogDebug(
                    "InterBus AmazonSqs: subscribed queue {QueueName} to topic {TopicName}, subscription ARN {SubscriptionArn})",
                    queueName,
                    topicName,
                    subscribeResponse.SubscriptionArn);
            }

            await SetQueuePolicyAsync(queueUrl, queueArn, descriptor, cancellationToken);

            logger.LogDebug("InterBus AmazonSqs: queue {QueueName} policy set", queueName);
        }
    }

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

            return topology.TopicArns.GetValueOrDefault(
                topology.BuildSnsTopicName(rawName));
        }).OfType<string>().Distinct().ToList();

        if (topicArns.Count == 0)
            return;

        var policy = $$"""
                       {
                           "Version": "2012-10-17",
                           "Statement": [
                               {
                                   "Effect": "Allow",
                                   "Principal": {
                                       "Service": "sns.amazonaws.com"
                                   },
                                   "Action": "sqs:SendMessage",
                                   "Resource": "{{queueArn}}",
                                   "Condition": {
                                       "StringEquals": {
                                           "aws:SourceArn": [{{string.Join(",", topicArns.Select(a => $"\"{a}\""))}}] 
                                       }
                                   }
                               }
                           ]
                       }
                       """;

        await sqsClient.SetQueueAttributesAsync(
            new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string> { ["Policy"] = policy },
            },
            cancellationToken);
    }

    private async Task<string> CreateQueueAsync(string queueName, CancellationToken cancellationToken)
    {
        var attributes = new Dictionary<string, string>
        {
            ["MessageRetentionPeriod"] = "1209600" // 14 days
        };

        if (transportOptions.EnableSqsEncryption)
        {
            attributes["SqsManagedSseEnabled"] = "true";
        }

        var response = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest { QueueName = queueName, Attributes = attributes },
            cancellationToken);

        var url = response.QueueUrl;
        topology.QueueUrls.TryAdd(queueName, url);
        return url;
    }

    private async Task<string> CreateTopicAsync(string topicName, CancellationToken cancellationToken)
    {
        var topicAttributes = new Dictionary<string, string>();

        if (transportOptions.EnableSnsEncryption)
        {
            topicAttributes["KmsMasterKeyId"] = "alias/aws/sns";
        }

        var response = await snsClient.CreateTopicAsync(
            new CreateTopicRequest { Name = topicName, Attributes = topicAttributes },
            cancellationToken);

        var arn = response.TopicArn;
        topology.TopicArns.TryAdd(topicName, arn);
        return arn;
    }

    private async Task VerifyConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = topology.BuildSqsQueueName(descriptor.QueueName);

            await VerifyQueueAsync(queueName, cancellationToken);
        }
    }

    private async Task<string> VerifyQueueAsync(string queueName, CancellationToken cancellationToken)
    {
        LogDebugVerifyingSqsQueue(queueName);

        var response = await sqsClient.GetQueueUrlAsync(
            new GetQueueUrlRequest { QueueName = queueName },
            cancellationToken);

        var url = response.QueueUrl;
        topology.QueueUrls.TryAdd(queueName, url);
        return url;
    }

    private async Task<string> VerifyTopicAsync(string topicName, CancellationToken cancellationToken)
    {
        // AWS SNS does not provide a simple endpoint for retrieving a topic by name
        // For now, this atrocity exists...

        LogDebugVerifyingSnsTopic(topicName);

        _callerIdentity ??= await stsClient.GetCallerIdentityAsync(new GetCallerIdentityRequest(), cancellationToken);

        var accountId = _callerIdentity.Account;

        var callerArn = _callerIdentity.Arn;
        var partition = callerArn.Split(':') is { Length: >= 2 } parts ? parts[1] : "aws";

        var region = snsClient.Config.RegionEndpoint?.SystemName
                     ?? throw new InvalidOperationException(
                         "Cannot determine the AWS region from the IAmazonSimpleNotificationService client configuration. Ensure a region has been configured on the AWS client (e.g. via environment variable AWS_DEFAULT_REGION or AWSOptions.Region)");

        var topicArn = $"arn:{partition}:sns:{region}:{accountId}:{topicName}";

        try
        {
            await snsClient.GetTopicAttributesAsync(
                new GetTopicAttributesRequest { TopicArn = topicArn },
                cancellationToken);
        }
        catch (AmazonServiceException ex) when (ex.ErrorCode is "NotFound")
        {
            throw new InvalidOperationException(
                $"SNS topic '{topicName}' (ARN: {topicArn}) was not found. Ensure the topic has been created before starting the application in {nameof(TopologyMode.VerifyOnly)} mode",
                ex);
        }

        topology.TopicArns.TryAdd(topicName, topicArn);
        return topicArn;
    }

    [LoggerMessage(LogLevel.Debug, "InterBus AmazonSqs: verifying SNS topic {TopicName}")]
    partial void LogDebugVerifyingSnsTopic(string topicName);

    [LoggerMessage(LogLevel.Debug, "InterBus AmazonSqs: verifying SQS queue {QueueName}")]
    partial void LogDebugVerifyingSqsQueue(string queueName);
}
