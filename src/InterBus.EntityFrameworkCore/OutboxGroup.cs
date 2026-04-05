namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Header row for a logical outbox group. Processors lock rows from this table
/// to claim work, then forward/delete all child <see cref="OutboxMessage"/> rows.
/// </summary>
public sealed class OutboxGroup
{
    /// <summary>Unique identifier for this outbox group.</summary>
    public required Guid Id { get; set; }

    /// <summary>When the group was created (UTC).</summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public ICollection<OutboxMessage> Messages { get; } = new List<OutboxMessage>();
}
