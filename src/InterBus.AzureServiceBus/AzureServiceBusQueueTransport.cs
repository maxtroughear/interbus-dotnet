using Azure.Messaging.ServiceBus;
using InterBus.Transporting;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Azure Service Bus backed <see cref="ITransport"/>.
/// </summary>
internal sealed class AzureServiceBusQueueTransport(
    AzureServiceBusClientProvider clientProvider,
    AzureServiceBusTransportOptions options)
    : ITransport
{
    /// <summary>URI scheme for Azure Service Bus queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "azureservicebusqueue";

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        if (context.Destination.Scheme != QueueScheme)
        {
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{QueueScheme}'",
                nameof(context));
        }

        var rawQueueName = RoutingConventions.GetDestinationName(context.Destination);
        var entityPath = AzureServiceBusNaming.SanitizeEntityName(options.QueuePrefix + rawQueueName);

        var sender = clientProvider.GetOrCreateSender(entityPath);
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
}
