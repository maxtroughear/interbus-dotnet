namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single schedule operation through the configure delegate passed
/// to <see cref="IMessageScheduler"/> scheduling methods.
/// Use the delegate to set headers (via <see cref="Headers"/>).
/// </summary>
/// <typeparam name="T">The message type being scheduled.</typeparam>
public sealed class ScheduleContext<T> where T : class
{
    /// <summary>
    /// The destination URI for this message. The scheme encodes the transport and
    /// destination type (e.g. <c>memorytopic://interbus/my-topic</c> for a topic
    /// fan-out, <c>memoryqueue://interbus/my-queue</c> for a point-to-point send).
    /// Use <see cref="RoutingConventions.GetDestinationName"/> to extract the name.
    /// </summary>
    public Uri Destination { get; }

    /// <summary>The message payload being scheduled.</summary>
    public T Payload { get; }

    /// <summary>
    /// The headers to attach to the scheduled message.
    /// Add, remove, or modify entries to control message routing and metadata.
    /// </summary>
    public MessageHeaders Headers { get; private set; }

    /// <summary>
    /// Initializes a <see cref="ScheduleContext{T}"/>.
    /// The framework creates instances internally, but the constructor is public to allow
    /// test implementations of <see cref="IMessageScheduler"/> to invoke configure delegates
    /// (e.g. in unit tests).
    /// </summary>
    public ScheduleContext(Uri destination, T payload, MessageHeaders headers)
    {
        Destination = destination;
        Payload = payload;
        Headers = headers;
    }
}
