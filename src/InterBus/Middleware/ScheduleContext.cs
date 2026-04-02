namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single schedule operation through the
/// <see cref="IScheduleMiddleware"/> pipeline.
/// Unlike <see cref="PublishContext{T}"/>, this context operates on an already-serialized
/// payload, allowing middleware to inspect or mutate the raw bytes and headers
/// (e.g. to implement the Claim Check pattern for large scheduled payloads).
/// </summary>
public sealed class ScheduleContext
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
    /// to offload the body to external storage and store only a reference (Claim Check).
    /// </summary>
    public byte[] Payload { get; set; }

    /// <summary>
    /// The headers attached to the scheduled message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    /// <summary>
    /// The UTC time at which the message should be delivered.
    /// Middleware may adjust this value.
    /// </summary>
    public DateTimeOffset ScheduledAt { get; set; }

    /// <summary>
    /// Initializes a <see cref="ScheduleContext"/>.
    /// The framework creates instances internally, but the constructor is public to allow
    /// test implementations of <see cref="IScheduleMiddleware"/>.
    /// </summary>
    public ScheduleContext(Uri destination, byte[] payload, MessageHeaders headers, DateTimeOffset scheduledAt)
    {
        Destination = destination;
        Payload = payload;
        Headers = headers;
        ScheduledAt = scheduledAt;
    }
}
