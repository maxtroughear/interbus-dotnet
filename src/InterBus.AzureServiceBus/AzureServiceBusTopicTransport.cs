using Azure.Messaging.ServiceBus;
using InterBus.Transporting;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Azure Service Bus backed <see cref="ITransport"/>.
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
internal sealed class AzureServiceBusTopicTransport(
    AzureServiceBusClientProvider clientProvider,
    ITopologyBuilder topology)
    : ITransport
{
    /// <summary>URI scheme for Azure Service Bus topic (fan-out) destinations.</summary>
    public const string TopicScheme = "azureservicebustopic";

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        if (context.Destination.Scheme != TopicScheme)
        {
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{TopicScheme}'",
                nameof(context));
        }

        var entityPath = await topology.EnsureTopicAsync(context.Destination, cancellationToken);

        var sender = clientProvider.GetOrCreateSender(entityPath);
        var message = AzureServiceBusMessageFactory.Create(context.Payload, context.Headers);
        if (context.Delay is { } delay && delay > TimeSpan.Zero)
            message.ScheduledEnqueueTime = DateTimeOffset.UtcNow + delay;

        await sender.SendMessageAsync(message, cancellationToken);
    }

}
