namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single publish or send operation through the
/// <see cref="IPublishMiddleware"/> pipeline.
/// Middleware may inspect or mutate <see cref="Payload"/> and <see cref="Headers"/>
/// (e.g. to implement the Claim Check pattern) before the terminal step forwards the
/// message to the transport.
/// </summary>
public sealed class PublishContext
{
    /// <summary>
    /// The destination name: a topic name when <see cref="IsTopic"/> is <c>true</c>,
    /// or a queue name when <see cref="IsTopic"/> is <c>false</c>.
    /// </summary>
    public string Destination { get; }

    /// <summary>
    /// <c>true</c> if the message is being published to a topic (fan-out);
    /// <c>false</c> if it is being sent directly to a queue (point-to-point).
    /// </summary>
    public bool IsTopic { get; }

    /// <summary>The serialized message payload. Middleware may replace this value.</summary>
    public string Payload { get; set; }

    /// <summary>
    /// The headers attached to the outgoing message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    internal PublishContext(string destination, bool isTopic, string payload, MessageHeaders headers)
    {
        Destination = destination;
        IsTopic = isTopic;
        Payload = payload;
        Headers = headers;
    }
}
