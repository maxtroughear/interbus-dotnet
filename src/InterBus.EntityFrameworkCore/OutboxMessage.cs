namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Represents a message stored in the transactional outbox. Messages are written here
/// by the outbox publish middleware and forwarded to the transport by the background
/// outbox processor.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>Unique identifier for this outbox entry.</summary>
    public Guid Id { get; set; } = Guid.NewGuid();

    /// <summary>
    /// Identifies the group of messages that were published together in the same DI scope
    /// (e.g. a single HTTP request or unit-of-work). All messages in the same group are
    /// fetched and forwarded atomically by the background processor.
    /// </summary>
    public Guid GroupId { get; set; }

    /// <summary>The serialized destination URI (e.g. <c>memorytopic://interbus/my-topic</c>).</summary>
    public string Destination { get; set; } = string.Empty;

    /// <summary>The serialized message payload.</summary>
    public byte[] Payload { get; set; } = [];

    /// <summary>JSON-serialized message headers.</summary>
    public string HeadersJson { get; set; } = string.Empty;

    /// <summary>
    /// Optional delay to pass to the transport when forwarding this message.
    /// The transport (not the outbox) is responsible for honouring the delay.
    /// </summary>
    public TimeSpan? Delay { get; set; }

    /// <summary>When the message was added to the outbox (UTC).</summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
