using InterBus.Transport;
using Microsoft.Extensions.Logging;

namespace InterBus.InMemory;

internal sealed class InMemoryTransport(
    InMemoryBroker broker,
    InMemoryTopologyBuilder topologyBuilder,
    ILogger<InMemoryTransport> logger)
    : ITransport
{
    public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        if (context.Delay.HasValue)
        {
            _ = DeliverAfterDelayAsync(context, cancellationToken);
            return Task.CompletedTask;
        }

        return DeliverCoreAsync(context, cancellationToken);
    }

    private async Task DeliverAfterDelayAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(context.Delay!.Value, cancellationToken);
            await DeliverCoreAsync(context, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Delivery cancelled (e.g. host shutting down or caller token cancelled). Silently discard.
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "InterBus InMemory: unhandled error delivering delayed message to {Destination}",
                context.Destination);
        }
    }

    private async Task DeliverCoreAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var name = RoutingConventions.GetDestinationName(context.Destination);
        var envelope = new InMemoryEnvelope(context.Payload, context.Headers);

        if (context.Destination.Scheme == InMemoryBroker.TopicScheme)
        {
            await topologyBuilder.EnsureTopicAsync(name, cancellationToken);
            await broker.PublishToTopicAsync(name, envelope, cancellationToken);
            return;
        }

        if (context.Destination.Scheme == InMemoryBroker.QueueScheme)
        {
            await topologyBuilder.EnsureQueueAsync(name, cancellationToken);
            await broker.SendToQueueAsync(name, envelope, cancellationToken);
            return;
        }

        throw new ArgumentException(
            $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
            $"Expected '{InMemoryBroker.TopicScheme}' or '{InMemoryBroker.QueueScheme}'.",
            nameof(context));
    }
}
