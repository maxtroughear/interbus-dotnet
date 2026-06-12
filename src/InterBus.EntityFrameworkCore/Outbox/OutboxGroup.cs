// ReSharper disable EntityFramework.ModelValidation.UnlimitedStringLength - set in ModelBuilder

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Header row for a logical outbox group. Processors claim the group's shard (via
/// <see cref="OutboxLock"/>) and then forward/delete all child <see cref="OutboxMessage"/> rows
/// in creation order, guaranteeing FIFO delivery for the same
/// <see cref="Shard"/>.
/// </summary>
public sealed class OutboxGroup
{
    /// <summary>Unique identifier for this outbox group.</summary>
    public required Guid Id { get; init; }

    /// <summary>When the group was created (UTC).</summary>
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Zero-based shard index. All groups with the same shard are processed in creation order
    /// by at most one processor at a time, guaranteeing FIFO within the shard.
    /// Defaults to <c>0</c> (single-shard / global FIFO).
    /// </summary>
    public int Shard { get; init; }

    public ICollection<OutboxMessage> Messages { get; } = new List<OutboxMessage>();
}
