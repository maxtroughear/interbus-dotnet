using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.Consuming;
using Microsoft.Extensions.Logging;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Declares and verifies Azure Service Bus topology for both consumer startup and producer send
/// paths. This keeps the message pump focused on processing while topology concerns are shared
/// by the pump and transport
/// </summary>
internal sealed class AzureServiceBusTopologyBuilder(
    AzureServiceBusTransportOptions options,
    AzureServiceBusClientProvider clientProvider,
    InterBusOptions busOptions,
    ILogger<AzureServiceBusTopologyBuilder> logger)
    : ITopologyBuilder
{
    private static readonly TimeSpan QueueLockDuration = TimeSpan.FromSeconds(60);

    private ConcurrentDictionary<string, Task> Topics { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, Task> Queues { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, Task> Subscriptions { get; } = new(StringComparer.Ordinal);

    public async Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var rawQueueName = descriptor.QueueName;

            var queueName = BuildQueueName(rawQueueName);
            await EnsureQueueExistsAsync(descriptor, queueName, cancellationToken);

            foreach (var messageType in descriptor.MessageTypes)
            {
                var rawTopicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var topicName = BuildTopicName(rawTopicName);
                await EnsureTopicExistsAsync(topicName, cancellationToken);

                var subscriptionName = BuildSubscriptionName(rawQueueName);
                await EnsureForwardingSubscriptionExistsAsync(
                    topicName,
                    subscriptionName,
                    queueName,
                    cancellationToken);
            }
        }
    }

    public async Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        var topicName = BuildTopicName(rawTopicName);
        await EnsureTopicExistsAsync(topicName, cancellationToken);
        return topicName;
    }

    /// <summary>
    /// Creates a session-enabled Azure Service Bus queue with short retention for request/reply
    /// exchanges. Returns a tuple of (rawQueueName, sanitizedFullName). Idempotent — silently
    /// succeeds if the queue already exists.
    /// </summary>
    public async Task<(string RawName, string FullName)> EnsureSessionQueueAsync(
        string rawQueueName,
        CancellationToken cancellationToken)
    {
        var queueName = BuildQueueName(rawQueueName);
        await EnsureWithCacheAsync(
            Queues,
            queueName,
            async ct =>
            {
                var admin = clientProvider.AdminClient;

                try
                {
                    await admin.CreateQueueAsync(new CreateQueueOptions(queueName)
                    {
                        LockDuration = QueueLockDuration,
                        RequiresSession = true,
                        // Short retention: replies are consumed exactly once and discarded.
                        // If the requestor times out or is restarted before receiving the reply,
                        // the message is automatically cleaned up.
                        DefaultMessageTimeToLive = TimeSpan.FromMinutes(10),
                        // One delivery attempt only: replies are idempotent from the sender's
                        // perspective but only consumed once, so dead-lettering after the first
                        // failure avoids unbounded redeliveries.
                        MaxDeliveryCount = 1,
                    }, ct);

                    logger.LogDebug(
                        "InterBus AzureServiceBus: created session reply queue {QueueName}",
                        queueName);
                }
                catch (ServiceBusException ex)
                    when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
                {
                    // Queue already exists — fine.
                }
            },
            cancellationToken);

        return (rawQueueName, queueName);
    }

    private Task EnsureQueueExistsAsync(
        ConsumerDescriptor descriptor,
        string queueName,
        CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(
            Queues,
            queueName,
            async ct =>
            {
                var admin = clientProvider.AdminClient;

                try
                {
                    await admin.CreateQueueAsync(new CreateQueueOptions(queueName)
                    {
                        LockDuration = QueueLockDuration,
                        MaxDeliveryCount = Math.Max(1, descriptor.MaxDeliveryCount),
                        RequiresSession = descriptor.IsFifo,
                    }, ct);

                    logger.LogDebug("InterBus AzureServiceBus: created queue {QueueName}", queueName);
                }
                catch (ServiceBusException ex)
                    when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
                {
                    // Queue already exists.
                }
            },
            cancellationToken);

    private Task EnsureTopicExistsAsync(string topicName, CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(
            Topics,
            topicName,
            async ct =>
            {
                var admin = clientProvider.AdminClient;

                try
                {
                    await admin.CreateTopicAsync(topicName, ct);
                    logger.LogDebug("InterBus AzureServiceBus: created topic {TopicName}", topicName);
                }
                catch (ServiceBusException ex)
                    when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
                {
                    // Topic already exists.
                }
            },
            cancellationToken);

    private Task EnsureForwardingSubscriptionExistsAsync(
        string topicName,
        string subscriptionName,
        string forwardToQueue,
        CancellationToken cancellationToken)
    {
        var key = topicName + "|" + subscriptionName;
        return EnsureWithCacheAsync(
            Subscriptions,
            key,
            async ct =>
            {
                var admin = clientProvider.AdminClient;

                try
                {
                    var subscriptionOptions = new CreateSubscriptionOptions(topicName, subscriptionName)
                    {
                        ForwardTo = forwardToQueue,
                    };

                    await admin.CreateSubscriptionAsync(subscriptionOptions, ct);

                    logger.LogDebug(
                        "InterBus AzureServiceBus: created subscription {SubscriptionName} on topic {TopicName} forwarding to queue {QueueName}",
                        subscriptionName,
                        topicName,
                        forwardToQueue);
                }
                catch (ServiceBusException ex)
                    when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
                {
                    // Subscription already exists.
                }
            },
            cancellationToken);
    }

    private static async Task EnsureWithCacheAsync(
        ConcurrentDictionary<string, Task> cache,
        string key,
        Func<CancellationToken, Task> factory,
        CancellationToken cancellationToken)
    {
        var task = cache.GetOrAdd(key, _ => factory(CancellationToken.None));

        try
        {
            await task.WaitAsync(cancellationToken);
        }
        catch
        {
            if (task.IsFaulted || task.IsCanceled)
                cache.TryRemove(key, out _);

            throw;
        }
    }

    private string BuildTopicName(string rawTopicName) =>
        AzureServiceBusNaming.SanitizeEntityName(options.TopicPrefix + rawTopicName);

    private string BuildQueueName(string rawQueueName) =>
        AzureServiceBusNaming.SanitizeEntityName(options.QueuePrefix + rawQueueName);

    private string BuildSubscriptionName(string rawQueueName) =>
        AzureServiceBusNaming.SanitizeSubscriptionName(options.QueuePrefix + rawQueueName);
}
