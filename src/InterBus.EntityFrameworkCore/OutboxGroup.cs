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

    /// <summary>
    /// When the group becomes eligible for processing (UTC). A <c>null</c> value means the group
    /// is immediately eligible. Set by the outbox middleware when messages are published from
    /// within a consumer's consume handler, to avoid the outbox processor picking up the group
    /// before the consumer's own transaction has had time to commit.
    /// </summary>
    public DateTime? VisibleAt { get; set; }

    public ICollection<OutboxMessage> Messages { get; } = new List<OutboxMessage>();
}
