namespace InterBus.InMemory;

/// <summary>
/// Singleton broker that owns all <see cref="InMemoryTopic"/> and <see cref="InMemoryQueue"/> instances.
/// Implements <see cref="IInterBusTransport"/> so the core <c>MessagePublisher</c> can dispatch through it.
/// Initialised from <see cref="InterBusOptions"/> at construction time.
/// Each consumer class is assigned exactly one queue, subscribed to all topics that consumer handles.
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
    public Task PublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        if (_topics.TryGetValue(topicName, out var topic))
            return topic.FanOutAsync(new InMemoryEnvelope(payload, headers), cancellationToken);

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        return _queues.TryGetValue(queueName, out var queue)
            ? queue.WriteAsync(new InMemoryEnvelope(payload, headers), cancellationToken).AsTask()
            : Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay > TimeSpan.Zero)
            await Task.Delay(delay, cancellationToken);

        await SendToQueueAsync(queueName, payload, headers, cancellationToken);
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
