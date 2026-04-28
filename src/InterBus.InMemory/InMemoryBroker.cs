using System.Collections.Concurrent;
using InterBus.Consuming;

namespace InterBus.InMemory;

/// <summary>
/// Singleton broker that owns all <see cref="InMemoryTopic"/> and <see cref="InMemoryQueue"/> instances.
/// Queue descriptors are sourced from <see cref="InterBusOptions"/> to support queue send-path ensures.
/// </summary>
public sealed class InMemoryBroker : IDisposable
{
    /// <summary>URI scheme for in-memory topic (fan-out) destinations.</summary>
    public const string TopicScheme = "memorytopic";

    /// <summary>URI scheme for in-memory queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "memoryqueue";

    private readonly ConcurrentDictionary<string, InMemoryTopic> _topics = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ConsumerDescriptor> _consumerDescriptorsByQueue = new(StringComparer.Ordinal);
    private bool _disposed;

    public InMemoryBroker(InterBusOptions options)
    {
        foreach (var descriptor in options.Consumers)
            _consumerDescriptorsByQueue[descriptor.QueueName] = descriptor;
    }

    internal Task PublishToTopicAsync(string topicName, InMemoryEnvelope envelope, CancellationToken cancellationToken)
    {
        var topic = EnsureTopicInternal(topicName);
        return topic.FanOutAsync(envelope, cancellationToken);
    }

    internal Task SendToQueueAsync(string queueName, InMemoryEnvelope envelope, CancellationToken cancellationToken)
    {
        var queue = TryEnsureQueue(queueName);
        return queue is null
            ? Task.CompletedTask
            : queue.WriteAsync(envelope, cancellationToken).AsTask();
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
