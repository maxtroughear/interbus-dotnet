namespace InterBus;

/// <summary>
/// Carries all the data required for a single transport send operation.
/// Passed to <see cref="IInterBusTransport.SendAsync"/> so that all relevant properties
/// (including the optional <see cref="Delay"/>) are available to the transport implementation
/// without requiring future interface changes.
/// </summary>
public sealed class TransportSendContext
{
    /// <summary>
    /// The destination URI for this message. The scheme encodes the transport and
    /// destination type (e.g. <c>memorytopic://interbus/my-topic</c> for a topic
    /// fan-out, <c>memoryqueue://interbus/my-queue</c> for a point-to-point send).
    /// Use <see cref="RoutingConventions.GetDestinationName"/> to extract the name.
    /// </summary>
    public Uri Destination { get; }

    /// <summary>The serialised message payload.</summary>
    public byte[] Payload { get; }

    /// <summary>The message headers.</summary>
    public MessageHeaders Headers { get; }

    /// <summary>
    /// Optional delay before the message becomes visible to consumers.
    /// When <c>null</c> the message is delivered immediately (default behaviour).
    /// Transport implementations that support deferred visibility (e.g. PostgreSQL
    /// <c>visible_at</c>) will honour this value; transports that do not may ignore it.
    /// </summary>
    public TimeSpan? Delay { get; }

    public TransportSendContext(Uri destination, byte[] payload, MessageHeaders headers, TimeSpan? delay)
    {
        Destination = destination;
        Payload = payload;
        Headers = headers;
        Delay = delay;
    }
}
