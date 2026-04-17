using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Declares and verifies Azure Service Bus topology for both consumer startup and producer send
/// paths. This keeps the message pump focused on processing while topology concerns are shared
/// by the pump and transport.
/// </summary>
internal sealed class AzureServiceBusTopology(
    AzureServiceBusTransportOptions options,
    AzureServiceBusClientProvider clientProvider, 
    InterBusOptions busOptions,
    ILogger<AzureServiceBusTopology> logger)
{
    private ConcurrentDictionary<string, byte> Topics { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, byte> Queues { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, byte> Subscriptions { get; } = new(StringComparer.Ordinal);
    
    internal async Task DeclareConsumerTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = BuildQueueName(descriptor.QueueName);
            await EnsureQueueExistsAsync(queueName, cancellationToken);

            foreach (var messageType in descriptor.MessageTypes)
            {
                var rawTopicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var topicName = BuildTopicName(rawTopicName);
                await EnsureTopicExistsAsync(topicName, cancellationToken);

                var subscriptionName = BuildSubscriptionName(descriptor.QueueName);
                await EnsureForwardingSubscriptionExistsAsync(
                    topicName,
                    subscriptionName,
                    queueName,
                    cancellationToken);
            }
        }
    }

    internal async Task<string> EnsureTopicEntityPathAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        var topicName = BuildTopicName(rawTopicName);
        await EnsureTopicExistsAsync(topicName, cancellationToken);
        return topicName;
    }

    internal async Task<string> EnsureQueueEntityPathAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        var queueName = BuildQueueName(rawQueueName);
        await EnsureQueueExistsAsync(queueName, cancellationToken);
        return queueName;
    }

    private async Task EnsureQueueExistsAsync(string queueName, CancellationToken cancellationToken)
    {
        if (Queues.ContainsKey(queueName))
            return;

        var admin = clientProvider.AdminClient;

        try
        {
            await admin.CreateQueueAsync(new CreateQueueOptions(queueName)
            {
                LockDuration = TimeSpan.FromSeconds(60),
            }, cancellationToken);

            logger.LogDebug("InterBus AzureServiceBus: created queue {QueueName}", queueName);
        }
        catch (ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Queue already exists.
        }

        Queues.TryAdd(queueName, 0);
    }

    private async Task EnsureTopicExistsAsync(string topicName, CancellationToken cancellationToken)
    {
        if (Topics.ContainsKey(topicName))
            return;

        var admin = clientProvider.AdminClient;

        try
        {
            await admin.CreateTopicAsync(topicName, cancellationToken);
            logger.LogDebug("InterBus AzureServiceBus: created topic {TopicName}", topicName);
        }
        catch (ServiceBusException ex)
            when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
        {
            // Topic already exists.
        }

        Topics.TryAdd(topicName, 0);
    }

    private async Task EnsureForwardingSubscriptionExistsAsync(
        string topicName,
        string subscriptionName,
        string forwardToQueue,
        CancellationToken cancellationToken)
    {
        var key = topicName + "|" + subscriptionName;
        if (Subscriptions.ContainsKey(key))
            return;

        var admin = clientProvider.AdminClient;

        try
        {
            var subscriptionOptions = new CreateSubscriptionOptions(topicName, subscriptionName)
            {
                ForwardTo = forwardToQueue,
            };

            await admin.CreateSubscriptionAsync(subscriptionOptions, cancellationToken);

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

        Subscriptions.TryAdd(key, 0);
    }

    private string BuildTopicName(string rawTopicName) =>
        AzureServiceBusNaming.SanitizeEntityName(options.TopicPrefix + rawTopicName);

    private string BuildQueueName(string rawQueueName) =>
        AzureServiceBusNaming.SanitizeEntityName(options.QueuePrefix + rawQueueName);

    private string BuildSubscriptionName(string rawQueueName) =>
        AzureServiceBusNaming.SanitizeSubscriptionName(options.QueuePrefix + rawQueueName);
}

