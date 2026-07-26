using System.Collections.Concurrent;
using System.Text.Json;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consuming;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

/// <summary>
/// Responsible for declaring the SNS/SQS topology required by registered InterBus consumers:
/// one SQS queue per consumer, one SNS topic per handled message type, and subscriptions
/// binding each queue to its topics
/// </summary>
public sealed partial class SnsSqsTopologyBuilder(
    IAmazonSimpleNotificationService snsClient,
    IAmazonSQS sqsClient,
    IAmazonSecurityTokenService stsClient,
    SnsSqsNaming naming,
    SnsSqsTransportOptions transportOptions,
    InterBusOptions busOptions,
    ILogger<SnsSqsTopologyBuilder> logger)
    : ITopologyBuilder
{
    private GetCallerIdentityResponse? _callerIdentity;
    private ConcurrentDictionary<string, Task<string>> EnsuredTopics { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, Task<string>> EnsuredQueues { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Application-level cache: sanitised topic name → SNS Topic ARN
    /// </summary>
    internal ConcurrentDictionary<string, string> TopicArns { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Application-level cache: sanitised queue name → SQS Queue URL
    /// </summary>
    internal ConcurrentDictionary<string, string> QueueUrls { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Application-level cache: sanitised queue name → SQS Queue ARN
    /// </summary>
    internal ConcurrentDictionary<string, string> QueueArns { get; } =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Declares (or verifies) the consumer topology for every registered consumer, populating the
    /// <see cref="SnsSqsNaming"/> caches.
    /// <para>
    /// In <see cref="TopologyMode.CreateOrVerify"/> mode this creates SQS queues, SNS topics,
    /// subscriptions (raw or SNS-serialised), and queue resource policies (idempotent).
    /// </para>
    /// <para>
    /// In <see cref="TopologyMode.ResolveOnly"/> mode only the queue URL is resolved. Topic verification is deferred
    /// until the first publish via <see cref="EnsureTopicAsync"/>.
    /// </para>
    /// </summary>
    public async Task BuildConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        if (transportOptions.TopologyMode == TopologyMode.ResolveOnly)
            await ResolveConsumerTopologyAsync(cancellationToken);
        else
            await CreateConsumerTopologyAsync(cancellationToken);
    }

    public async Task<string> EnsureTopicAsync(Uri topic, CancellationToken cancellationToken)
    {
        var snsTopicName = naming.BuildSnsTopicName(topic);

        if (TopicArns.TryGetValue(snsTopicName, out var cachedArn))
            return cachedArn;

        return await EnsureWithCacheAsync(
            EnsuredTopics,
            snsTopicName,
            ct => transportOptions.TopologyMode == TopologyMode.ResolveOnly
                ? ResolveTopicAsync(snsTopicName, ct)
                : CreateTopicAsync(snsTopicName, ct),
            cancellationToken);
    }

    public async Task<string> EnsureQueueAsync(Uri queue, CancellationToken cancellationToken)
    {
        var sqsQueueName = naming.BuildSqsQueueName(queue);

        if (QueueUrls.TryGetValue(sqsQueueName, out var cachedUrl))
            return cachedUrl;

        return await EnsureWithCacheAsync(
            EnsuredQueues,
            sqsQueueName,
            ct => transportOptions.TopologyMode == TopologyMode.ResolveOnly
                ? ResolveQueueAsync(sqsQueueName, ct)
                : CreateQueueAsync(sqsQueueName, ct),
            cancellationToken);
    }

    private async Task CreateConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = naming.BuildSqsQueueName(descriptor.Queue);
            var deadLetterQueueName = naming.BuildDeadLetterQueueName(descriptor.Queue);

            await CreateQueueAsync(deadLetterQueueName, cancellationToken);
            var deadLetterQueueArn = QueueArns[deadLetterQueueName];

            var queueUrl = await CreateQueueAsync(
                queueName,
                cancellationToken,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["RedrivePolicy"] = JsonSerializer.Serialize(new
                    {
                        deadLetterTargetArn = deadLetterQueueArn,
                        maxReceiveCount = Math.Max(1, descriptor.MaxDeliveryCount)
                    }),
                });

            var queueArn = QueueArns[queueName];

            logger.LogDebug(
                "InterBus AmazonSqs: declared queue {QueueName} for consumer {ConsumerType}, URL {QueueUrl}",
                queueName,
                descriptor.ConsumerType.Name,
                queueUrl);

            foreach (var messageType in descriptor.MessageTypes)
            {
                var topic = busOptions.GetPublisherDescriptor(messageType).Topic;
                var topicName = naming.BuildSnsTopicName(topic);
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
                            ["RawMessageDelivery"] = "true"
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
            var topic = busOptions.GetPublisherDescriptor(t).Topic;

            return TopicArns.GetValueOrDefault(naming.BuildSnsTopicName(topic));
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

    internal async Task<string> CreateQueueAsync(
        string sqsQueueName,
        CancellationToken cancellationToken,
        IDictionary<string, string>? additionalAttributes = null)
    {
        var isFifo = sqsQueueName.EndsWith(".fifo", StringComparison.Ordinal);

        var attributes = new Dictionary<string, string>
        {
            ["MessageRetentionPeriod"] = "1209600" // 14 days
        };

        if (isFifo)
        {
            // High-throughput FIFO: each message group ID gets its own throughput quota.
            attributes["FifoQueue"] = "true";
            attributes["ContentBasedDeduplication"] = "true";
            attributes["DeduplicationScope"] = "messageGroup";
            attributes["FifoThroughputLimit"] = "perMessageGroupId";
        }

        if (additionalAttributes is not null)
        {
            foreach (var attribute in additionalAttributes)
                attributes[attribute.Key] = attribute.Value;
        }

        if (transportOptions.EnableSqsEncryption)
        {
            // Default to SSE-SQS when encryption is enabled.
            attributes["SqsManagedSseEnabled"] = "true";
        }

        var response = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest { QueueName = sqsQueueName, Attributes = attributes },
            cancellationToken);

        var url = response.QueueUrl;
        QueueUrls.TryAdd(sqsQueueName, url);

        await ResolveQueueAsync(sqsQueueName, cancellationToken);

        return url;
    }

    private async Task<string> CreateTopicAsync(string snsTopicName, CancellationToken cancellationToken)
    {
        var isFifo = snsTopicName.EndsWith(".fifo", StringComparison.Ordinal);
        var topicAttributes = new Dictionary<string, string>();

        if (isFifo)
        {
            topicAttributes["FifoTopic"] = "true";
            topicAttributes["ContentBasedDeduplication"] = "true";
        }

        if (transportOptions.EnableSnsEncryption)
        {
            topicAttributes["KmsMasterKeyId"] = "alias/aws/sns";
        }

        var response = await snsClient.CreateTopicAsync(
            new CreateTopicRequest { Name = snsTopicName, Attributes = topicAttributes },
            cancellationToken);

        var arn = response.TopicArn;
        TopicArns.TryAdd(snsTopicName, arn);
        return arn;
    }

    private async Task ResolveConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        var (accountId, partition) = await ResolveCallerIdentityAsync(cancellationToken);

        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = naming.BuildSqsQueueName(descriptor.Queue);

            LogDebugVerifyingSqsQueue(queueName);

            var sqsRegion = GetRegion(sqsClient.Config);

            var url = $"{sqsClient.Config.ServiceURL}/{accountId}/{queueName}";
            var queueArn = $"arn:{partition}:sqs:{sqsRegion}:{accountId}:{queueName}";

            QueueUrls.TryAdd(queueName, url);
            QueueArns.TryAdd(queueName, queueArn);
        }
    }

    public async Task<string> ResolveQueueAsync(string sqsQueueName, CancellationToken cancellationToken)
    {
        var (accountId, partition) = await ResolveCallerIdentityAsync(cancellationToken);

        var sqsRegion = GetRegion(sqsClient.Config);

        var url = $"{sqsClient.Config.ServiceURL}/{accountId}/{sqsQueueName}";
        var queueArn = $"arn:{partition}:sqs:{sqsRegion}:{accountId}:{sqsQueueName}";

        QueueUrls.TryAdd(sqsQueueName, url);
        QueueArns.TryAdd(sqsQueueName, queueArn);

        return url;
    }

    public async Task<string> ResolveTopicAsync(string snsTopicName, CancellationToken cancellationToken)
    {
        var (accountId, partition) = await ResolveCallerIdentityAsync(cancellationToken);

        var region = GetRegion(snsClient.Config);

        var topicArn = $"arn:{partition}:sns:{region}:{accountId}:{snsTopicName}";
        TopicArns.TryAdd(snsTopicName, topicArn);
        return topicArn;
    }

    private async Task<(string AccountId, string Partition)> ResolveCallerIdentityAsync(
        CancellationToken cancellationToken)
    {
        _callerIdentity ??= await stsClient.GetCallerIdentityAsync(new GetCallerIdentityRequest(), cancellationToken);
        var callerArn = _callerIdentity.Arn;
        var partition = callerArn.Split(':') is { Length: >= 2 } parts ? parts[1] : "aws";
        return (_callerIdentity.Account, partition);
    }

    /// <summary>
    /// Returns the effective region name for an AWS client config, falling back to
    /// <see cref="Amazon.Runtime.ClientConfig.AuthenticationRegion"/> when
    /// <see cref="Amazon.Runtime.ClientConfig.RegionEndpoint"/> is cleared by an explicit
    /// <see cref="Amazon.Runtime.ClientConfig.ServiceURL"/> override (the LocalStack pattern).
    /// </summary>
    private static string GetRegion(Amazon.Runtime.IClientConfig config)
    {
        if (config.RegionEndpoint?.SystemName is { Length: > 0 } r)
            return r;

        if (config.AuthenticationRegion is { } authRegion)
            return authRegion;

        throw new InvalidOperationException(
            "Cannot determine the AWS region from the client configuration. " +
            "Ensure a region has been configured on the AWS client " +
            "(e.g. via environment variable AWS_DEFAULT_REGION or AWSOptions.Region).");
    }

    [LoggerMessage(LogLevel.Debug, "InterBus AmazonSqs: verifying SQS queue {QueueName}")]
    partial void LogDebugVerifyingSqsQueue(string queueName);

    private static async Task<TValue> EnsureWithCacheAsync<TValue>(
        ConcurrentDictionary<string, Task<TValue>> cache,
        string key,
        Func<CancellationToken, Task<TValue>> factory,
        CancellationToken cancellationToken)
    {
        var task = cache.GetOrAdd(key, _ => factory(CancellationToken.None));

        try
        {
            return await task.WaitAsync(cancellationToken);
        }
        catch
        {
            if (task.IsFaulted || task.IsCanceled)
                cache.TryRemove(key, out _);

            throw;
        }
    }
}
