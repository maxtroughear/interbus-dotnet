namespace InterBus.InMemory;

/// <summary>
/// Singleton broker that owns all <see cref="InMemoryTopic"/> and <see cref="InMemoryQueue"/> instances.
/// Implements <see cref="IInterBusTransport"/> so the core <c>MessagePublisher</c> can dispatch through it.
/// Initialised from <see cref="InterBusOptions"/> at construction time.
/// Each consumer class is assigned exactly one queue, which is subscribed to all topics that consumer handles.
/// </summary>
public sealed class InMemoryBroker : IInterBusTransport, IDisposable
{
    private readonly Dictionary<string, InMemoryTopic> _topics = new(StringComparer.Ordinal);
    private readonly Dictionary<string, InMemoryQueue> _queues = new(StringComparer.Ordinal);
    private bool _disposed;

    public InMemoryBroker(InterBusOptions options)
    {
        // Group descriptors by queue name so each consumer class gets exactly one queue.
        var byConsumer = options.Consumers.GroupBy(d => d.QueueName);

        foreach (var group in byConsumer)
        {
            // One queue per consumer class, handling all of that consumer's message types.
            var queue = new InMemoryQueue(group.AsEnumerable());
            _queues[group.Key] = queue;

            // Subscribe the queue to every topic it handles (one topic per message type).
            foreach (var descriptor in group)
            {
                if (!_topics.TryGetValue(descriptor.TopicName, out var topic))
                {
                    topic = new InMemoryTopic(descriptor.TopicName);
                    _topics[descriptor.TopicName] = topic;
                }

                topic.AddSubscriber(queue);
            }
        }
    }

    /// <inheritdoc/>
    public Task PublishToTopicAsync(string topicName, string payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        if (_topics.TryGetValue(topicName, out var topic))
            return topic.FanOutAsync(new InMemoryEnvelope(topicName, payload, headers), cancellationToken);

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task SendToQueueAsync(string queueName, string topicName, string payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        if (_queues.TryGetValue(queueName, out var queue))
            return queue.WriteAsync(new InMemoryEnvelope(topicName, payload, headers), cancellationToken).AsTask();

        return Task.CompletedTask;
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
