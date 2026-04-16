using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
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
    IAmazonSecurityTokenService stsClient,
    SnsSqsClientProvider clientProvider,
    SnsSqsTransportOptions transportOptions,
    InterBusOptions busOptions,
    ILogger<SnsSqsTopologySetup> logger)
{
    private GetCallerIdentityResponse? _callerIdentity;

    /// <summary>
    /// Declares (or verifies) the consumer topology for every registered consumer, populating the
    /// <see cref="SnsSqsClientProvider"/> caches.
    /// <para>
    /// In <see cref="TopologyMode.CreateOrVerify"/> mode this creates SQS queues, SNS topics, raw-delivery
    /// subscriptions, and queue resource policies (idempotent).
    /// </para>
    /// <para>
    /// In <see cref="TopologyMode.VerifyOnly"/> mode only the queue URL is resolved. Topic verification is deferred
    /// until the first publish via <see cref="EnsureTopicArnAsync"/>.
    /// </para>
    /// </summary>
    internal async Task DeclareConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        if (transportOptions.TopologyMode == TopologyMode.VerifyOnly)
            await VerifyConsumerTopologyAsync(cancellationToken);
        else
            await CreateOrVerifyConsumerTopologyAsync(cancellationToken);
    }
    
    internal async Task<string> EnsureTopicArnAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        var topicName = clientProvider.BuildSnsTopicName(rawTopicName);

        if (clientProvider.TopicArns.TryGetValue(topicName, out var cachedArn))
            return cachedArn;

        return transportOptions.TopologyMode == TopologyMode.VerifyOnly
            ? await VerifyTopicArnAsync(topicName, cancellationToken)
            : await CreateTopicAsync(topicName, cancellationToken);
    }

    internal async Task<string> EnsureQueueUrlAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        var queueName = clientProvider.BuildSqsQueueName(rawQueueName);

        if (clientProvider.QueueUrls.TryGetValue(queueName, out var cachedUrl))
            return cachedUrl;

        return transportOptions.TopologyMode == TopologyMode.VerifyOnly
            ? await VerifyQueueUrlAsync(queueName, cancellationToken)
            : await CreateQueueAsync(queueName, cancellationToken);
    }

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

                var topicName = clientProvider.BuildSnsTopicName(rawTopicName);

                logger.LogDebug(
                    "InterBus AmazonSqs: declaring SNS topic {TopicName}",
                    topicName);

                var topicArn = await CreateTopicAsync(topicName, cancellationToken);

                var subscribeResponse = await snsClient.SubscribeAsync(
                    new SubscribeRequest
                    {
                        TopicArn = topicArn,
                        Protocol = "sqs",
                        Endpoint = queueArn,
                        Attributes = new Dictionary<string, string>
                        {
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

            await SetQueuePolicyAsync(queueUrl, queueArn, descriptor, cancellationToken);
        }
    }
    
    private async Task<string> CreateQueueAsync(string queueName, CancellationToken cancellationToken)
    {
        var attributes = new Dictionary<string, string>
        {
            ["MessageRetentionPeriod"] = "1209600" // 14 days
        };

        // if (transportOptions.EnableSqsEncryption)
        // {
        //     // SSE-SQS: SQS-managed encryption. No KMS charges; no cross-service key policy
        //     // required. The KmsMasterKeyId attribute is intentionally left unset so that
        //     // SQS uses its own managed key rather than a customer KMS key.
        //     attributes["SqsManagedSseEnabled"] = "true";
        // }

        var response = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest { QueueName = queueName, Attributes = attributes },
            cancellationToken);

        var url = response.QueueUrl;
        clientProvider.QueueUrls.TryAdd(queueName, url);
        return url;
    }

    private async Task<string> CreateTopicAsync(string topicName, CancellationToken cancellationToken)
    {
        var topicAttributes = new Dictionary<string, string>();

        // if (transportOptions.EnableSnsEncryption)
        // {
        //     // AWS managed key for SNS (alias/aws/sns). No customer-managed KMS key required.
        //     topicAttributes["KmsMasterKeyId"] = "alias/aws/sns";
        // }
        
        var response = await snsClient.CreateTopicAsync(
            new CreateTopicRequest { Name = topicName, Attributes = topicAttributes },
            cancellationToken);

        var arn = response.TopicArn;
        clientProvider.TopicArns.TryAdd(topicName, arn);
        return arn;
    }

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

    private async Task<string> VerifyQueueUrlAsync(string queueName, CancellationToken cancellationToken)
    {
        var response = await sqsClient.GetQueueUrlAsync(
            new GetQueueUrlRequest { QueueName = queueName },
            cancellationToken);

        var url = response.QueueUrl;
        clientProvider.QueueUrls.TryAdd(queueName, url);
        return url;
    }

    private async Task<string> VerifyTopicArnAsync(string topicName, CancellationToken cancellationToken)
    {
        logger.LogDebug("InterBus AmazonSqs: verifying SNS topic {TopicName}", topicName);

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

        clientProvider.TopicArns.TryAdd(topicName, topicArn);
        return topicArn;
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

            return clientProvider.TopicArns.GetValueOrDefault(
                clientProvider.BuildSnsTopicName(rawName));
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
            }, cancellationToken);
    }
}
