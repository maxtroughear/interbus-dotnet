using System.Threading.Channels;

namespace InterBus.InMemory;

/// <summary>
/// A Channel-backed Point-to-Point queue for a specific consumer and message type.
/// Each registered consumer gets one <see cref="InMemoryQueue"/> per message type it handles.
/// </summary>
internal sealed class InMemoryQueue
{
    private readonly Channel<byte[]> _channel = Channel.CreateUnbounded<byte[]>(
        new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });

    /// <summary>The consumer descriptor that owns this queue.</summary>
    public ConsumerDescriptor Descriptor { get; }

    internal InMemoryQueue(ConsumerDescriptor descriptor)
    {
        Descriptor = descriptor;
    }

    /// <summary>Enqueue a serialised payload.</summary>
    public ValueTask WriteAsync(byte[] payload, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(payload, cancellationToken);

    /// <summary>Read all messages as an async stream. Completes when the channel is closed.</summary>
    public IAsyncEnumerable<byte[]> ReadAllAsync(CancellationToken cancellationToken) =>
        _channel.Reader.ReadAllAsync(cancellationToken);

    /// <summary>Signal that no further messages will be written (called on shutdown).</summary>
    public void Complete() => _channel.Writer.TryComplete();
}
