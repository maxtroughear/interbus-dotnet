namespace InterBus.Middleware;

public class SendContext : IContext
{
    /// <summary>
    /// The destination URI for this message. The scheme encodes the transport and
    /// destination type (e.g. <c>memorytopic://interbus/my-topic</c> for a topic
    /// fan-out, <c>memoryqueue://interbus/my-queue</c> for a point-to-point send).
    /// Use <see cref="RoutingConventions.GetDestinationName"/> to extract the name.
    /// </summary>
    public Uri Destination { get; }

    /// <summary>
    /// The serialized message payload. Middleware may replace this value, for example
    /// to retrieve the original body from external storage (Claim Check).
    /// </summary>
    public byte[] Payload { get; set; }

    /// <summary>
    /// The headers attached to the message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    /// <summary>
    /// Optional delay before the message becomes visible to consumers
    /// </summary>
    public TimeSpan? Delay { get; }

    /// <summary>
    /// Initializes a <see cref="SendContext"/>.
    /// </summary>
    public SendContext(Uri destination, byte[] payload, MessageHeaders headers, TimeSpan? delay = null)
    {
        Destination = destination;
        Payload = payload;
        Headers = headers;
        Delay = delay;
    }
}
