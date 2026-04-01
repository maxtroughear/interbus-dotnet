namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single publish or send operation through the
/// <see cref="IPublishMiddleware"/> pipeline.
/// Middleware may inspect or mutate <see cref="Payload"/> and <see cref="Headers"/>
/// (e.g. to implement the Claim Check pattern) before the terminal step forwards the
/// message to the transport.
/// </summary>
public sealed class PublishContext<T>
{
    /// <summary>
    /// The destination URI for this message. The scheme encodes the transport and
    /// destination type (e.g. <c>memorytopic://interbus/my-topic</c> for a topic
    /// fan-out, <c>memoryqueue://interbus/my-queue</c> for a point-to-point send).
    /// Use <see cref="RoutingConventions.GetDestinationName"/> to extract the name.
    /// </summary>
    public Uri Destination { get; }

    /// <summary>The serialized message payload. Middleware may replace this value.</summary>
    public T Payload { get; set; }

    /// <summary>
    /// The headers attached to the outgoing message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    internal PublishContext(Uri destination, T payload, MessageHeaders headers)
    {
        Destination = destination;
        Payload = payload;
        Headers = headers;
    }
}
