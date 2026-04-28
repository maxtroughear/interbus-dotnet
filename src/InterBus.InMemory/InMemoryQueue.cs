using System.Threading.Channels;
using InterBus.Consuming;

namespace InterBus.InMemory;

/// <summary>
/// A Channel-backed Point-to-Point queue for a single consumer class.
/// Each registered consumer gets exactly one <see cref="InMemoryQueue"/>.
/// Routing to the correct <c>Consume</c> overload is handled by the consumer's
/// <see cref="ConsumerDescriptor.Dispatch"/> delegate using the
/// <see cref="InterBusHeaders.MessageType"/> header - no topic-name lookup required here.
/// </summary>
internal sealed class InMemoryQueue
{
    private readonly Channel<InMemoryEnvelope> _channel = Channel.CreateUnbounded<InMemoryEnvelope>(
        new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });

    /// <summary>The consumer descriptor that owns this queue.</summary>
    public ConsumerDescriptor Descriptor { get; }

    internal InMemoryQueue(ConsumerDescriptor descriptor)
    {
        Descriptor = descriptor;
    }

    /// <summary>Enqueue a serialised payload with its headers.</summary>
    public ValueTask WriteAsync(InMemoryEnvelope envelope, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(envelope, cancellationToken);

    /// <summary>Read all envelopes as an async stream. Completes when the channel is closed.</summary>
    public IAsyncEnumerable<InMemoryEnvelope> ReadAllAsync(CancellationToken cancellationToken) =>
        _channel.Reader.ReadAllAsync(cancellationToken);

    /// <summary>Signal that no further messages will be written (called on shutdown).</summary>
    public void Complete() => _channel.Writer.TryComplete();
}
