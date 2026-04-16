using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System.Collections.Concurrent;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Singleton service that manages the <see cref="ServiceBusClient"/> and
/// <see cref="ServiceBusAdministrationClient"/> for the Azure Service Bus transport.
/// <para>
/// Maintains a cache of <see cref="ServiceBusSender"/> instances keyed by entity path so that
/// senders are reused across DI scopes. <see cref="ServiceBusSender"/> is thread-safe and
/// designed to be long-lived.
/// </para>
/// </summary>
internal sealed class AzureServiceBusClientProvider : IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders
        = new(StringComparer.Ordinal);

    public AzureServiceBusClientProvider(AzureServiceBusTransportOptions options)
    {
        _client = new ServiceBusClient(options.ConnectionString);
        var adminConnectionString = options.AdminConnectionString ?? options.ConnectionString;
        AdminClient = new ServiceBusAdministrationClient(adminConnectionString);
    }

    /// <summary>The administration client for managing topics, queues, and subscriptions.</summary>
    public ServiceBusAdministrationClient AdminClient { get; }

    /// <summary>
    /// Returns a cached <see cref="ServiceBusSender"/> for the given entity path, creating one
    /// on first use.
    /// </summary>
    public ServiceBusSender GetOrCreateSender(string entityPath) =>
        _senders.GetOrAdd(entityPath, _client.CreateSender);

    /// <summary>Creates a <see cref="ServiceBusProcessor"/> for the given queue.</summary>
    public ServiceBusProcessor CreateQueueProcessor(string queueName, ServiceBusProcessorOptions processorOptions) =>
        _client.CreateProcessor(queueName, processorOptions);

    /// <summary>Creates a <see cref="ServiceBusProcessor"/> for the given topic subscription.</summary>
    public ServiceBusProcessor CreateSubscriptionProcessor(
        string topicName,
        string subscriptionName,
        ServiceBusProcessorOptions processorOptions) =>
        _client.CreateProcessor(topicName, subscriptionName, processorOptions);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        foreach (var sender in _senders.Values)
            await sender.DisposeAsync();
        _senders.Clear();

        await _client.DisposeAsync();
    }
}
