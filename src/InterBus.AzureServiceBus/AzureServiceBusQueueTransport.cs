using System.Web;
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

        var queueName = RoutingConventions.GetDestinationName(context.Destination);
        var azureQueueName = AzureServiceBusNaming.SanitizeEntityName(options.QueuePrefix + queueName);

        // A ?session= query parameter encodes the target session ID (used by reply endpoints
        // to route replies into the correct ASB session without a per-request queue).
        var query = HttpUtility.ParseQueryString(context.Destination.Query);
        var sessionId = query["session"];

        var sender = clientProvider.GetOrCreateSender(azureQueueName);
        var message = BuildMessage(context, sessionId);
        await sender.SendMessageAsync(message, cancellationToken);
    }

    private static ServiceBusMessage BuildMessage(TransportSendContext context, string? sessionId = null)
    {
        var message = new ServiceBusMessage(context.Payload);

        foreach (var header in context.Headers)
            message.ApplicationProperties[header.Key] = header.Value;

        if (context.Delay.HasValue && context.Delay.Value > TimeSpan.Zero)
            message.ScheduledEnqueueTime = DateTimeOffset.UtcNow + context.Delay.Value;

        // Set SessionId for session-enabled queues (FIFO ordering and request/reply sessions).
        // The MessageGroupId header is always stamped by Publisher (defaults to CorrelationId),
        // so setting it here is safe even on non-session queues — ASB ignores it.
        if (sessionId is not null)
            message.SessionId = sessionId;
        else if (context.Headers.TryGetValue(InterBusHeaders.MessageGroupId, out var groupId))
            message.SessionId = groupId;

        return message;
    }
}
