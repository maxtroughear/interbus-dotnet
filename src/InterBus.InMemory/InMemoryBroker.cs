using InterBus.Transport;
using Microsoft.Extensions.Logging;

namespace InterBus.InMemory;

/// <summary>
/// Singleton broker that owns all <see cref="InMemoryTopic"/> and <see cref="InMemoryQueue"/> instances.
/// Implements <see cref="IInterBusTransport"/> so the core <c>MessagePublisher</c> can dispatch through it.
/// Initialised from <see cref="InterBusOptions"/> at construction time.
/// Each consumer class is assigned exactly one queue, subscribed to all topics that consumer handles.
/// </summary>
public sealed class InMemoryBroker : IInterBusTransport, IDisposable
{
    /// <summary>URI scheme for in-memory topic (fan-out) destinations.</summary>
    public const string TopicScheme = "memorytopic";

    /// <summary>URI scheme for in-memory queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "memoryqueue";

    private readonly Dictionary<string, InMemoryTopic> _topics = new(StringComparer.Ordinal);
    private readonly Dictionary<string, InMemoryQueue> _queues = new(StringComparer.Ordinal);
    private readonly ILogger<InMemoryBroker> _logger;
    private bool _disposed;

    public InMemoryBroker(InterBusOptions options, ILogger<InMemoryBroker> logger)
    {
        _logger = logger;

        foreach (var descriptor in options.Consumers)
        {
            // One queue per consumer class.
            var queue = new InMemoryQueue(descriptor);
            _queues[descriptor.QueueName] = queue;

            // Subscribe the queue to every topic the consumer handles

            foreach (var type in descriptor.MessageTypes)
            {
                // Resolve topics from descriptor types
                var topicName = options.TryGetTopicName(type, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(type);

                if (!_topics.TryGetValue(topicName, out var topic))
                {
                    topic = new InMemoryTopic(topicName);
                    _topics[topicName] = topic;
                }

                topic.AddSubscriber(queue);
            }
        }
    }

    /// <inheritdoc/>
    public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        if (context.Delay.HasValue)
        {
            // Schedule delivery in the background so the caller is not blocked for the delay duration.
            _ = DeliverAfterDelayAsync(context, cancellationToken);
            return Task.CompletedTask;
        }

        return DeliverCoreAsync(context, cancellationToken);
    }

    /// <summary>Waits for the configured delay then forwards the message to the destination.</summary>
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
            _logger.LogError(
                ex,
                "InterBus InMemory: unhandled error delivering delayed message to {Destination}",
                context.Destination);
        }
    }

    private Task DeliverCoreAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var name = RoutingConventions.GetDestinationName(context.Destination);

        if (context.Destination.Scheme == TopicScheme)
        {
            if (_topics.TryGetValue(name, out var topic))
                return topic.FanOutAsync(new InMemoryEnvelope(context.Payload, context.Headers), cancellationToken);
            return Task.CompletedTask;
        }

        if (context.Destination.Scheme == QueueScheme)
        {
            return _queues.TryGetValue(name, out var queue)
                ? queue.WriteAsync(new InMemoryEnvelope(context.Payload, context.Headers), cancellationToken).AsTask()
                : Task.CompletedTask;
        }

        throw new ArgumentException(
            $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
            $"Expected '{TopicScheme}' or '{QueueScheme}'.",
            nameof(context));
    }

    /// <summary>Returns all registered queues. Used by the message pump to start consumers.</summary>
    internal IReadOnlyCollection<InMemoryQueue> GetAllQueues() => _queues.Values;

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var queue in _queues.Values)
            queue.Complete();
    }
}
