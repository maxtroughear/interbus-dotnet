using System.Web;

namespace InterBus.AzureServiceBus;

internal sealed class AzureServiceBusSchedulingProvider(
    AzureServiceBusClientProvider clients,
    AzureServiceBusTransportOptions options,
    ITopologyBuilder topology) : IMessageSchedulingProvider
{
    public async Task<string> ScheduleAsync(Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        string entityPath;
        string? virtualQueueId = null;
        switch (destination.Scheme)
        {
            case AzureServiceBusQueueTransport.QueueScheme:
                entityPath = AzureServiceBusNaming.SanitizeEntityName(
                    options.QueuePrefix + RoutingConventions.GetDestinationName(destination));
                virtualQueueId = HttpUtility.ParseQueryString(destination.Query)["vq"];
                break;
            case AzureServiceBusTopicTransport.TopicScheme:
                entityPath = await topology.EnsureTopicAsync(destination, cancellationToken);
                break;
            default:
                throw new ArgumentException($"Unsupported destination URI scheme '{destination.Scheme}'", nameof(destination));
        }

        var message = AzureServiceBusMessageFactory.Create(payload, headers, virtualQueueId);
        var sequence = await clients.GetOrCreateSender(entityPath)
            .ScheduleMessageAsync(message, scheduledAt, cancellationToken);
        return AzureServiceBusScheduleId.Encode(entityPath, sequence);
    }

    public Task CancelAsync(string scheduleId, CancellationToken cancellationToken)
    {
        var (entityPath, sequence) = AzureServiceBusScheduleId.Decode(scheduleId);
        return clients.GetOrCreateSender(entityPath).CancelScheduledMessageAsync(sequence, cancellationToken);
    }
}
