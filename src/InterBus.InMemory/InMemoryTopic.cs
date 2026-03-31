namespace InterBus.InMemory;

/// <summary>
/// Publish-Subscribe Channel. Fans out every published payload to all registered subscriber queues.
/// </summary>
internal sealed class InMemoryTopic(string name)
{
    private readonly string _name = name;
    private readonly List<InMemoryQueue> _subscribers = [];

    /// <summary>Subscribe a queue to this topic. Called at startup.</summary>
    public void AddSubscriber(InMemoryQueue queue) => _subscribers.Add(queue);

    /// <summary>Fan out an envelope to all subscriber queues.</summary>
    public async Task FanOutAsync(InMemoryEnvelope envelope, CancellationToken cancellationToken)
    {
        foreach (var queue in _subscribers)
        {
            await queue.WriteAsync(envelope, cancellationToken);
        }
    }
}
