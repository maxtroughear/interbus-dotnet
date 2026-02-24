namespace InterBus.InMemory;

/// <summary>
/// Publish-Subscribe Channel. Fans out every published payload to all registered subscriber queues.
/// </summary>
internal sealed class InMemoryTopic
{
    private readonly List<InMemoryQueue> _subscribers = [];

    public InMemoryTopic(string name)
    {
        _ = name; // stored for potential future diagnostics
    }

    /// <summary>Subscribe a queue to this topic. Called at startup.</summary>
    public void AddSubscriber(InMemoryQueue queue) => _subscribers.Add(queue);

    /// <summary>Fan out a payload to all subscriber queues.</summary>
    public async Task FanOutAsync(byte[] payload, CancellationToken cancellationToken)
    {
        foreach (var queue in _subscribers)
        {
            await queue.WriteAsync(payload, cancellationToken);
        }
    }
}
