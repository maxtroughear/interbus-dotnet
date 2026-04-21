using System.Collections.Concurrent;
using InterBus.Consumer;
using InterBus.Transport;
using Microsoft.Extensions.Logging;

namespace InterBus.InMemory;

/// <summary>
/// Singleton broker that owns all <see cref="InMemoryTopic"/> and <see cref="InMemoryQueue"/> instances.
/// Implements <see cref="ITransport"/> so the core <c>MessagePublisher</c> can dispatch through it.
/// Topology is initialised from <see cref="InterBusOptions"/> by <see cref="BuildTopologyAsync"/>.
/// Each consumer class is assigned exactly one queue, subscribed to all topics that consumer handles.
/// </summary>
public sealed class InMemoryBroker : ITransport, ITopologyBuilder, IDisposable
{
    /// <summary>URI scheme for in-memory topic (fan-out) destinations.</summary>
    public const string TopicScheme = "memorytopic";

    /// <summary>URI scheme for in-memory queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "memoryqueue";

    private readonly ConcurrentDictionary<string, InMemoryTopic> _topics = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ConsumerDescriptor> _consumerDescriptorsByQueue = new(StringComparer.Ordinal);
    private readonly InterBusOptions _options;
    private readonly object _topologyLock = new();
    private bool _topologyBuilt;
    private readonly ILogger<InMemoryBroker> _logger;
    private bool _disposed;

    public InMemoryBroker(InterBusOptions options, ILogger<InMemoryBroker> logger)
    {
        _options = options;
        _logger = logger;

        foreach (var descriptor in options.Consumers)
            _consumerDescriptorsByQueue[descriptor.QueueName] = descriptor;
    }

    public Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        lock (_topologyLock)
        {
            if (_topologyBuilt)
                return Task.CompletedTask;

            foreach (var descriptor in _options.Consumers)
            {
                var queue = EnsureQueue(descriptor);
                foreach (var type in descriptor.MessageTypes)
                {
                    var topicName = _options.TryGetTopicName(type, out var custom)
                        ? custom
                        : RoutingConventions.TopicName(type);
                    EnsureTopic(topicName).AddSubscriber(queue);
                }
            }

            _topologyBuilt = true;
        }

        return Task.CompletedTask;
    }

    public Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        EnsureTopic(rawTopicName);
        return Task.FromResult(rawTopicName);
    }

    public Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        if (_consumerDescriptorsByQueue.TryGetValue(rawQueueName, out var descriptor))
            EnsureQueue(descriptor);
        return Task.FromResult(rawQueueName);
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
            var topic = EnsureTopic(name);
            return topic.FanOutAsync(new InMemoryEnvelope(context.Payload, context.Headers), cancellationToken);
        }

        if (context.Destination.Scheme == QueueScheme)
        {
            if (_consumerDescriptorsByQueue.TryGetValue(name, out var descriptor))
            {
                var queue = EnsureQueue(descriptor);
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

    private InMemoryTopic EnsureTopic(string topicName) =>
        _topics.GetOrAdd(topicName, _ => new InMemoryTopic());

    private InMemoryQueue EnsureQueue(ConsumerDescriptor descriptor) =>
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
