namespace InterBus.EFCore;

/// <summary>
/// Represents a message stored in the transactional outbox. Messages are written here
/// by the outbox publish middleware and forwarded to the transport by the background
/// outbox processor.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>Unique identifier for this outbox entry.</summary>
    public Guid Id { get; set; } = Guid.NewGuid();

    /// <summary>The serialized destination URI (e.g. <c>memorytopic://interbus/my-topic</c>).</summary>
    public string Destination { get; set; } = string.Empty;

    /// <summary>The serialized message payload.</summary>
    public byte[] Payload { get; set; } = [];

    /// <summary>JSON-serialized message headers.</summary>
    public string HeadersJson { get; set; } = string.Empty;

    /// <summary>
    /// The earliest UTC time the message should be forwarded to the transport.
    /// When <c>null</c> the message is forwarded immediately. Maps to the
    /// <see cref="InterBus.Middleware.PublishContext{T}.Delay"/> set by the caller.
    /// </summary>
    public DateTime? VisibleAt { get; set; }

    /// <summary>When the message was added to the outbox (UTC).</summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// When the message was successfully forwarded to the transport (UTC).
    /// <c>null</c> means the message is pending or in-flight. Only set when
    /// <see cref="OutboxOptions.KeepProcessedMessages"/> is <c>true</c>; otherwise
    /// the row is deleted after forwarding.
    /// </summary>
    public DateTime? ProcessedAt { get; set; }
}
