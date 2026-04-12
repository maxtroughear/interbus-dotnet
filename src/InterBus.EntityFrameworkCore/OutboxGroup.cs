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
    /// When the group becomes eligible for processing (UTC). Groups with a <c>VisibleAt</c>
    /// timestamp in the future are ignored by the outbox processor until that time has elapsed.
    /// Set to <see cref="DateTime.UtcNow"/> for immediate eligibility, or
    /// <c>UtcNow + <see cref="OutboxOptions.ConsumerVisibilityTimeout"/></c> for consumer-context
    /// publishes to prevent the processor from racing with inline delivery.
    /// </summary>
    public required DateTime VisibleAt { get; set; }

    public ICollection<OutboxMessage> Messages { get; } = new List<OutboxMessage>();
}
