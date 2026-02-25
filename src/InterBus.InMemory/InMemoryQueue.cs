using System.Threading.Channels;

namespace InterBus.InMemory;

/// <summary>
/// A Channel-backed Point-to-Point queue for a single consumer class.
/// Each registered consumer gets exactly one <see cref="InMemoryQueue"/>, regardless of how many
/// message types it handles. Routing to the correct handler is performed via <see cref="FindHandler"/>
/// using the <see cref="InMemoryEnvelope.TopicName"/> on each arriving envelope.
/// </summary>
internal sealed class InMemoryQueue
{
    private readonly Channel<InMemoryEnvelope> _channel = Channel.CreateUnbounded<InMemoryEnvelope>(
        new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });

    private readonly IReadOnlyDictionary<string, ConsumerDescriptor> _handlersByTopic;

    /// <summary>The name of this queue (derived from the consumer type by convention).</summary>
    public string QueueName { get; }

    /// <summary>The concrete consumer type that owns this queue.</summary>
    public Type ConsumerType { get; }

    internal InMemoryQueue(IEnumerable<ConsumerDescriptor> descriptors)
    {
        var list = descriptors.ToList();
        QueueName = list[0].QueueName;
        ConsumerType = list[0].ConsumerType;
        _handlersByTopic = list.ToDictionary(d => d.TopicName, StringComparer.Ordinal);
    }

    /// <summary>
    /// Finds the descriptor whose topic matches <paramref name="topicName"/>.
    /// Returns <c>null</c> if no handler is registered for that topic on this queue.
    /// </summary>
    public ConsumerDescriptor? FindHandler(string topicName) =>
        _handlersByTopic.TryGetValue(topicName, out var d) ? d : null;

    /// <summary>Enqueue a serialised payload with its headers.</summary>
    public ValueTask WriteAsync(InMemoryEnvelope envelope, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(envelope, cancellationToken);

    /// <summary>Read all envelopes as an async stream. Completes when the channel is closed.</summary>
    public IAsyncEnumerable<InMemoryEnvelope> ReadAllAsync(CancellationToken cancellationToken) =>
        _channel.Reader.ReadAllAsync(cancellationToken);

    /// <summary>Signal that no further messages will be written (called on shutdown).</summary>
    public void Complete() => _channel.Writer.TryComplete();
}
