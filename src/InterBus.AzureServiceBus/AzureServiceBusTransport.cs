using Azure.Messaging.ServiceBus;
using InterBus.Transport;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Azure Service Bus backed <see cref="IInterBusTransport"/>.
/// <para>
/// When <see cref="SendAsync"/> is called with an <c>azureservicebustopic://</c> destination URI,
/// the message is published to an Azure Service Bus topic. Every subscription on that topic
/// receives a copy, delivering to all bound consumers.
/// </para>
/// <para>
/// When called with an <c>azureservicebusqueue://</c> destination URI, the message is sent
/// directly to the named Azure Service Bus queue for point-to-point delivery.
/// </para>
/// <para>
/// Delayed delivery is supported via <see cref="TransportSendContext.Delay"/>: when set, the
/// message's <c>ScheduledEnqueueTime</c> is set to <c>UtcNow + Delay</c> so the broker holds
/// the message until the scheduled time before making it available to consumers.
/// </para>
/// <para>
/// InterBus message headers are stored as Azure Service Bus
/// <see cref="ServiceBusMessage.ApplicationProperties"/>.
/// </para>
/// </summary>
public sealed class AzureServiceBusTransport : IInterBusTransport
{
    /// <summary>URI scheme for Azure Service Bus topic (fan-out) destinations.</summary>
    public const string TopicScheme = "azureservicebustopic";

    /// <summary>URI scheme for Azure Service Bus queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "azureservicebusqueue";

    private readonly AzureServiceBusClientProvider _clientProvider;
    private readonly AzureServiceBusTransportOptions _options;

    internal AzureServiceBusTransport(
        AzureServiceBusClientProvider clientProvider,
        AzureServiceBusTransportOptions options)
    {
        _clientProvider = clientProvider;
        _options = options;
    }

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        string entityPath;

        if (context.Destination.Scheme == TopicScheme)
        {
            var topicName = RoutingConventions.GetDestinationName(context.Destination);
            entityPath = AzureServiceBusNaming.SanitizeEntityName(_options.TopicPrefix + topicName);
        }
        else if (context.Destination.Scheme == QueueScheme)
        {
            var queueName = RoutingConventions.GetDestinationName(context.Destination);
            entityPath = AzureServiceBusNaming.SanitizeEntityName(_options.QueuePrefix + queueName);
        }
        else
        {
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{TopicScheme}' or '{QueueScheme}'.",
                nameof(context));
        }

        var sender = _clientProvider.GetOrCreateSender(entityPath);
        var message = BuildMessage(context);
        await sender.SendMessageAsync(message, cancellationToken);
    }

    private static ServiceBusMessage BuildMessage(TransportSendContext context)
    {
        var message = new ServiceBusMessage(context.Payload);

        foreach (var header in context.Headers)
            message.ApplicationProperties[header.Key] = header.Value;

        if (context.Delay.HasValue && context.Delay.Value > TimeSpan.Zero)
            message.ScheduledEnqueueTime = DateTimeOffset.UtcNow + context.Delay.Value;

        return message;
    }

    internal static MessageHeaders ParseApplicationProperties(
        IReadOnlyDictionary<string, object?> applicationProperties)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var kv in applicationProperties)
        {
            var value = kv.Value switch
            {
                string str => str,
                _ => kv.Value?.ToString(),
            };
            if (value is not null)
                dict[kv.Key] = value;
        }

        return new MessageHeaders(dict);
    }
}
