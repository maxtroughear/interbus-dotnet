using InterBus.Middleware;

namespace InterBus.Publisher;

/// <summary>
/// Carries the state of a single publish or send operation through the
/// <see cref="IPublishMiddleware"/> pipeline.
/// Middleware may inspect or mutate <see cref="Message"/> and <see cref="Headers"/>
/// (e.g. to implement the Claim Check pattern) before the terminal step forwards the
/// message to the transport.
/// </summary>
public sealed class PublishContext<T> : IContext
{
    /// <summary>
    /// The destination URI for this message. The scheme encodes the transport and
    /// destination type (e.g. <c>memorytopic://interbus/my-topic</c> for a topic
    /// fan-out, <c>memoryqueue://interbus/my-queue</c> for a point-to-point send).
    /// Use <see cref="RoutingConventions.GetDestinationName"/> to extract the name.
    /// </summary>
    public Uri Destination { get; }

    /// <summary>The message. Middleware may replace this value.</summary>
    public T Message { get; set; }

    /// <summary>
    /// The headers attached to the outgoing message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    /// <summary>
    /// Optional delay before the message becomes visible to consumers.
    /// When <c>null</c> the message is delivered immediately (default behaviour).
    /// Set via the configure delegate passed to <see cref="IPublisher"/> publish/send methods.
    /// </summary>
    public TimeSpan? Delay { get; set; }

    /// <summary>
    /// Initializes a <see cref="PublishContext{T}"/>.
    /// The framework creates instances internally, but the constructor is public to allow
    /// test implementations of <see cref="IPublisher"/> to invoke configure delegates
    /// (e.g. in unit tests for middleware or redelivery handlers).
    /// </summary>
    public PublishContext(Uri destination, T message, MessageHeaders headers, TimeSpan? delay = null)
    {
        Destination = destination;
        Message = message;
        Headers = headers;
        Delay = delay;
    }
}
