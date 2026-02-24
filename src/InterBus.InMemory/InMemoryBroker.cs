namespace InterBus.InMemory;

/// <summary>
/// Singleton broker that owns all <see cref="InMemoryTopic"/> and <see cref="InMemoryQueue"/> instances.
/// Implements <see cref="IInterBusTransport"/> so the core <c>MessagePublisher</c> can dispatch through it.
/// Initialised from <see cref="InterBusOptions"/> at construction time.
/// </summary>
public sealed class InMemoryBroker : IInterBusTransport, IDisposable
{
    private readonly Dictionary<string, InMemoryTopic> _topics = new(StringComparer.Ordinal);
    private readonly Dictionary<string, InMemoryQueue> _queues = new(StringComparer.Ordinal);
    private bool _disposed;

    public InMemoryBroker(InterBusOptions options)
    {
        foreach (var descriptor in options.Consumers)
        {
            // Ensure the topic exists.
            if (!_topics.TryGetValue(descriptor.TopicName, out var topic))
            {
                topic = new InMemoryTopic(descriptor.TopicName);
                _topics[descriptor.TopicName] = topic;
            }

            // Each consumer gets its own queue (Point-to-Point Channel).
            var queue = new InMemoryQueue(descriptor);
            _queues[descriptor.QueueName] = queue;

            // Subscribe the queue to the topic (Publish-Subscribe fan-out).
            topic.AddSubscriber(queue);
        }
    }

    /// <inheritdoc/>
    public Task PublishToTopicAsync(string topicName, byte[] payload, CancellationToken cancellationToken)
    {
        if (_topics.TryGetValue(topicName, out var topic))
            return topic.FanOutAsync(payload, cancellationToken);

        // No registered consumers — message is silently dropped.
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task SendToQueueAsync(string queueName, byte[] payload, CancellationToken cancellationToken)
    {
        if (_queues.TryGetValue(queueName, out var queue))
            return queue.WriteAsync(payload, cancellationToken).AsTask();

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
