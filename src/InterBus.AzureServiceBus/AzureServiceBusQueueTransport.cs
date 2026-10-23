using System.Web;
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

        var queueName = RoutingConventions.GetDestinationName(context.Destination);
        var azureQueueName = AzureServiceBusNaming.SanitizeEntityName(options.QueuePrefix + queueName);

        // A ?vq= query parameter encodes the target virtual queue ID (mapped to ASB SessionId
        // to route replies into the correct session without a per-request queue).
        var query = HttpUtility.ParseQueryString(context.Destination.Query);
        var virtualQueueId = query["vq"];

        var sender = clientProvider.GetOrCreateSender(azureQueueName);
        var message = AzureServiceBusMessageFactory.Create(context.Payload, context.Headers, virtualQueueId);
        if (context.Delay is { } delay && delay > TimeSpan.Zero)
            message.ScheduledEnqueueTime = DateTimeOffset.UtcNow + delay;

        await sender.SendMessageAsync(message, cancellationToken);
    }

}
