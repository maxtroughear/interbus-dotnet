// ReSharper disable EntityFramework.ModelValidation.UnlimitedStringLength - set in ModelBuilder

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Represents a message stored in the transactional outbox. Messages are written here
/// by the outbox publish middleware and forwarded to the transport by the background
/// outbox processor.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>Unique identifier for this outbox entry.</summary>
    public Guid Id { get; init; } = Guid.CreateVersion7();

    /// <summary>
    /// Foreign key to the outbox group/header row that owns this message.
    /// All messages in the same group are forwarded atomically.
    /// </summary>
    public required Guid GroupId { get; init; }

    public OutboxGroup Group { get; init; } = null!;

    /// <summary>The serialized destination URI (e.g. <c>memorytopic://interbus/my-topic</c>).</summary>
    public required string Destination { get; init; }

    /// <summary>The serialized message payload.</summary>
    public required byte[] Payload { get; init; }

    /// <summary>JSON-serialized message headers.</summary>
    public required string HeadersJson { get; init; }

    /// <summary>
    /// Optional delay to pass to the transport when forwarding this message.
    /// The transport (not the outbox) is responsible for honouring the delay.
    /// </summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>When the message was added to the outbox (UTC).</summary>
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
}
