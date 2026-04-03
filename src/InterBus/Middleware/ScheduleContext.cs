namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single scheduled-message delivery through the
/// <see cref="IScheduleMiddleware"/> pipeline, executed by
/// <see cref="SchedulePipelineExecutor"/> when the scheduled job fires.
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
    /// to retrieve the original body from external storage (Claim Check).
    /// </summary>
    public byte[] Payload { get; set; }

    /// <summary>
    /// The headers attached to the message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    /// <summary>
    /// Optional delay before the message becomes visible to consumers.
    /// When <c>null</c> the message is delivered immediately (default behaviour).
    /// Set by scheduling providers (e.g. <c>TransportDelaySchedulingProvider</c>) that
    /// forward the delay to the transport via <see cref="TransportSendContext.Delay"/>.
    /// Middleware may inspect or override this value.
    /// </summary>
    public TimeSpan? Delay { get; set; }

    /// <summary>
    /// Initializes a <see cref="ScheduleContext"/>.
    /// The framework creates instances internally, but the constructor is public to allow
    /// test implementations of <see cref="IScheduleMiddleware"/>.
    /// </summary>
    public ScheduleContext(Uri destination, byte[] payload, MessageHeaders headers)
    {
        Destination = destination;
        Payload = payload;
        Headers = headers;
    }
}
