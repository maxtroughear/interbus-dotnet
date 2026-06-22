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

    public async Task BuildConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = descriptor.QueueName;

            var azureQueueName = BuildQueueName(queueName);
            await EnsureQueueExistsAsync(descriptor, azureQueueName, cancellationToken);

            foreach (var messageType in descriptor.MessageTypes)
            {
                var topicName = busOptions.GetPublisherDescriptor(messageType).TopicName;

                var azureTopicName = BuildTopicName(topicName);
                await EnsureTopicExistsAsync(azureTopicName, cancellationToken);

                var subscriptionName = BuildSubscriptionName(azureQueueName);
                await EnsureForwardingSubscriptionExistsAsync(
                    azureTopicName,
                    subscriptionName,
                    azureQueueName,
                    RoutingConventions.MessageTypeString(messageType),
                    cancellationToken);
            }
        }
    }

    public async Task<string> EnsureTopicAsync(Uri topic, CancellationToken cancellationToken)
    {
        var topicName = RoutingConventions.GetDestinationName(topic);
        var azureTopicName = BuildTopicName(topicName);
        await EnsureTopicExistsAsync(azureTopicName, cancellationToken);
        return azureTopicName;
    }

    /// <summary>
    /// Creates a session-enabled Azure Service Bus queue with short retention for request/reply
    /// exchanges. Returns a tuple of (rawQueueName, sanitizedFullName). Idempotent — silently
    /// succeeds if the queue already exists.
    /// </summary>
    public async Task<string> EnsureSessionQueueAsync(
        string queueName,
        CancellationToken cancellationToken)
    {
        var azureQueueName = BuildQueueName(queueName);
        await EnsureWithCacheAsync(
            Queues,
            azureQueueName,
            async ct =>
            {
                var admin = clientProvider.AdminClient;

                try
                {
                    await admin.CreateQueueAsync(new CreateQueueOptions(azureQueueName)
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
                        azureQueueName);
                }
                catch (ServiceBusException ex)
                    when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
                {
                    // Queue already exists — fine.
                }
            },
            cancellationToken);

        return azureQueueName;
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

    private Task EnsureTopicExistsAsync(string azureTopicName, CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(
            Topics,
            azureTopicName,
            async ct =>
            {
                var admin = clientProvider.AdminClient;

                try
                {
                    await admin.CreateTopicAsync(azureTopicName, ct);
                    logger.LogDebug("InterBus AzureServiceBus: created topic {TopicName}", azureTopicName);
                }
                catch (ServiceBusException ex)
                    when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
                {
                    // Topic already exists.
                }
            },
            cancellationToken);

    private Task EnsureForwardingSubscriptionExistsAsync(
        string azureTopicName,
        string subscriptionName,
        string forwardToQueue,
        string messageTypeName,
        CancellationToken cancellationToken)
    {
        var key = azureTopicName + "|" + subscriptionName;
        return EnsureWithCacheAsync(
            Subscriptions,
            key,
            async ct =>
            {
                var admin = clientProvider.AdminClient;

                try
                {
                    var subscriptionOptions = new CreateSubscriptionOptions(azureTopicName, subscriptionName)
                    {
                        ForwardTo = forwardToQueue,
                    };

                    await admin.CreateSubscriptionAsync(subscriptionOptions, ct);
                    await EnsureSubscriptionRuleAsync(
                        admin,
                        azureTopicName,
                        subscriptionName,
                        messageTypeName,
                        ct);

                    logger.LogDebug(
                        "InterBus AzureServiceBus: created subscription {SubscriptionName} on topic {TopicName} forwarding to queue {QueueName}",
                        subscriptionName,
                        azureTopicName,
                        forwardToQueue);
                }
                catch (ServiceBusException ex)
                    when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
                {
                    await EnsureSubscriptionRuleAsync(
                        admin,
                        azureTopicName,
                        subscriptionName,
                        messageTypeName,
                        ct);
                }
            },
            cancellationToken);
    }

    private static async Task EnsureSubscriptionRuleAsync(
        ServiceBusAdministrationClient admin,
        string topicName,
        string subscriptionName,
        string messageTypeName,
        CancellationToken cancellationToken)
    {
        const string ruleName = "interbus-filter";
        var sqlFilter = new SqlRuleFilter(
            $"userProperties['{InterBusHeaders.MessageType}'] = '{EscapeSqlLiteral(messageTypeName)}'");

        try
        {
            var rule = await admin.GetRuleAsync(topicName, subscriptionName, ruleName, cancellationToken);

            rule.Value.Filter = sqlFilter;

            await admin.UpdateRuleAsync(topicName, subscriptionName, rule.Value, cancellationToken);
        }
        catch (ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            await admin.CreateRuleAsync(
                topicName,
                subscriptionName,
                new CreateRuleOptions(ruleName, sqlFilter),
                cancellationToken);
        }

        try
        {
            await admin.DeleteRuleAsync(topicName, subscriptionName, "$Default", cancellationToken);
        }
        catch (ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            // Default rule already removed.
        }
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

    private string BuildTopicName(string topicName) =>
        AzureServiceBusNaming.SanitizeEntityName(options.TopicPrefix + topicName);

    private string BuildQueueName(string queueName) =>
        AzureServiceBusNaming.SanitizeEntityName(options.QueuePrefix + queueName);

    private string BuildSubscriptionName(string queueName) =>
        AzureServiceBusNaming.SanitizeSubscriptionName(options.QueuePrefix + queueName);

    private static string EscapeSqlLiteral(string value) =>
        value.Replace("'", "''", StringComparison.Ordinal);
}
