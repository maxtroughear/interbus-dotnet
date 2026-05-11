using System.Collections.Concurrent;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consuming;
using Microsoft.Extensions.DependencyInjection;
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
    IServiceProvider serviceProvider,
    SnsSqsTopology topology,
    SnsSqsTransportOptions transportOptions,
    InterBusOptions busOptions,
    ILogger<SnsSqsTopologyBuilder> logger)
    : ITopologyBuilder
{
    private GetCallerIdentityResponse? _callerIdentity;
    private ConcurrentDictionary<string, Task<string>> EnsuredTopics { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, Task<string>> EnsuredQueues { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Declares (or verifies) the consumer topology for every registered consumer, populating the
    /// <see cref="SnsSqsTopology"/> caches.
    /// <para>
    /// In <see cref="TopologyMode.CreateOrVerify"/> mode this creates SQS queues, SNS topics,
    /// subscriptions (raw or SNS-serialised), and queue resource policies (idempotent).
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

        return await EnsureWithCacheAsync(
            EnsuredTopics,
            topicName,
            ct => transportOptions.TopologyMode == TopologyMode.VerifyOnly
                ? VerifyTopicAsync(topicName, ct)
                : CreateTopicAsync(topicName, ct),
            cancellationToken);
    }

    public async Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        var queueName = topology.BuildSqsQueueName(rawQueueName);

        if (topology.QueueUrls.TryGetValue(queueName, out var cachedUrl))
            return cachedUrl;

        return await EnsureWithCacheAsync(
            EnsuredQueues,
            queueName,
            ct => transportOptions.TopologyMode == TopologyMode.VerifyOnly
                ? VerifyQueueAsync(queueName, ct)
                : CreateQueueAsync(queueName, ct),
            cancellationToken);
    }

    private async Task CreateConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = topology.BuildSqsQueueName(descriptor.QueueName);
            var deadLetterQueueName = topology.BuildDeadLetterQueueName(descriptor.QueueName);

            var deadLetterQueueUrl = await CreateQueueAsync(deadLetterQueueName, cancellationToken);
            var deadLetterQueueArn = topology.QueueArns[deadLetterQueueUrl];

            var queueUrl = await CreateQueueAsync(
                queueName,
                cancellationToken,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["RedrivePolicy"] = JsonSerializer.Serialize(new
                    {
                        deadLetterTargetArn = deadLetterQueueArn,
                        maxReceiveCount = Math.Max(1, transportOptions.DeadLetterMaxReceiveCount).ToString(),
                    }),
                });

            var queueArn = topology.QueueArns[queueUrl];

            logger.LogDebug(
                "InterBus AmazonSqs: declared queue {QueueName} for consumer {ConsumerType}, URL {QueueUrl}",
                queueName,
                descriptor.ConsumerType.Name,
                queueUrl);

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

                var envelope = serviceProvider.GetKeyedService<ISnsSqsMessageEnvelope>(descriptor.ConsumerType)
                               ?? serviceProvider.GetRequiredService<ISnsSqsMessageEnvelope>();

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

    private async Task<string> CreateQueueAsync(
        string queueName,
        CancellationToken cancellationToken,
        IDictionary<string, string>? additionalAttributes = null)
    {
        var attributes = new Dictionary<string, string>
        {
            ["MessageRetentionPeriod"] = "1209600" // 14 days
        };

        if (additionalAttributes is not null)
        {
            foreach (var attribute in additionalAttributes)
                attributes[attribute.Key] = attribute.Value;
        }

        if (transportOptions.EnableSqsEncryption)
        {
            attributes["SqsManagedSseEnabled"] = "true";
        }

        var response = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest { QueueName = queueName, Attributes = attributes },
            cancellationToken);

        var url = response.QueueUrl;
        topology.QueueUrls.TryAdd(queueName, url);

        // Resolve and cache the queue ARN so it is available to EventBridge Scheduler targeting
        // and subscription setup without a separate API call.
        var attrs = await sqsClient.GetQueueAttributesAsync(
            new GetQueueAttributesRequest { QueueUrl = url, AttributeNames = ["QueueArn"] },
            cancellationToken);
        topology.QueueArns.TryAdd(url, attrs.QueueARN);

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

        // Resolve and cache the queue ARN for EventBridge Scheduler targeting.
        var attrs = await sqsClient.GetQueueAttributesAsync(
            new GetQueueAttributesRequest { QueueUrl = url, AttributeNames = ["QueueArn"] },
            cancellationToken);
        topology.QueueArns.TryAdd(url, attrs.QueueARN);

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
