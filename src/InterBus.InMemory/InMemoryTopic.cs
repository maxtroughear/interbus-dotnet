namespace InterBus.InMemory;

/// <summary>
/// Publish-Subscribe Channel. Fans out every published payload to all registered subscriber queues.
/// </summary>
internal sealed class InMemoryTopic
{
    private readonly List<InMemoryQueue> _subscribers = [];
    private readonly object _lock = new();

    /// <summary>Subscribe a queue to this topic. Called at startup.</summary>
    public void AddSubscriber(InMemoryQueue queue)
    {
        lock (_lock)
        {
            if (!_subscribers.Contains(queue))
                _subscribers.Add(queue);
        }
    }

    /// <summary>Fan out an envelope to all subscriber queues.</summary>
    public async Task FanOutAsync(InMemoryEnvelope envelope, CancellationToken cancellationToken)
    {
        InMemoryQueue[] subscribers;
        lock (_lock)
        {
            subscribers = _subscribers.ToArray();
        }

        foreach (var queue in subscribers)
        {
            await queue.WriteAsync(envelope, cancellationToken);
        }
    }
}
