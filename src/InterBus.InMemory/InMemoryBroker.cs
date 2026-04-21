using System.Collections.Concurrent;
using InterBus.Consumer;
using InterBus.Transport;
using Microsoft.Extensions.Logging;

namespace InterBus.InMemory;

/// <summary>
/// Singleton broker that owns all <see cref="InMemoryTopic"/> and <see cref="InMemoryQueue"/> instances.
/// Implements <see cref="ITransport"/> so the core <c>MessagePublisher</c> can dispatch through it.
/// Queue descriptors are sourced from <see cref="InterBusOptions"/> to support queue send-path ensures.
/// </summary>
public sealed class InMemoryBroker : ITransport, IDisposable
{
    /// <summary>URI scheme for in-memory topic (fan-out) destinations.</summary>
    public const string TopicScheme = "memorytopic";

    /// <summary>URI scheme for in-memory queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "memoryqueue";

    private readonly ConcurrentDictionary<string, InMemoryTopic> _topics = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ConsumerDescriptor> _consumerDescriptorsByQueue = new(StringComparer.Ordinal);
    private readonly ILogger<InMemoryBroker> _logger;
    private bool _disposed;

    public InMemoryBroker(InterBusOptions options, ILogger<InMemoryBroker> logger)
    {
        _logger = logger;

        foreach (var descriptor in options.Consumers)
            _consumerDescriptorsByQueue[descriptor.QueueName] = descriptor;
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
            var topic = EnsureTopicInternal(name);
            return topic.FanOutAsync(new InMemoryEnvelope(context.Payload, context.Headers), cancellationToken);
        }

        if (context.Destination.Scheme == QueueScheme)
        {
            if (_consumerDescriptorsByQueue.TryGetValue(name, out var descriptor))
            {
                var queue = EnsureQueueInternal(descriptor);
                return queue.WriteAsync(new InMemoryEnvelope(context.Payload, context.Headers), cancellationToken).AsTask();
            }
            return Task.CompletedTask;
        }

        throw new ArgumentException(
            $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
            $"Expected '{TopicScheme}' or '{QueueScheme}'.",
            nameof(context));
    }

    /// <summary>Returns all registered queues. Used by the message pump to start consumers.</summary>
    internal IReadOnlyCollection<InMemoryQueue> GetAllQueues() => _queues.Values.ToArray();

    internal InMemoryTopic EnsureTopic(string topicName) =>
        EnsureTopicInternal(topicName);

    internal InMemoryQueue? TryEnsureQueue(string queueName)
    {
        return _consumerDescriptorsByQueue.TryGetValue(queueName, out var descriptor)
            ? EnsureQueueInternal(descriptor)
            : null;
    }

    internal InMemoryQueue EnsureQueue(ConsumerDescriptor descriptor) =>
        EnsureQueueInternal(descriptor);

    private InMemoryTopic EnsureTopicInternal(string topicName) =>
        _topics.GetOrAdd(topicName, _ => new InMemoryTopic());

    private InMemoryQueue EnsureQueueInternal(ConsumerDescriptor descriptor) =>
        _queues.GetOrAdd(descriptor.QueueName, _ => new InMemoryQueue(descriptor));

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var queue in _queues.Values)
            queue.Complete();
    }
}
