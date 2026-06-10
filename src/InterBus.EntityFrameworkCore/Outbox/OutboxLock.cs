namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// One row per shard. Database-level lock providers lock this row (via
/// <c>FOR UPDATE SKIP LOCKED</c> / <c>UPDLOCK, READPAST</c>) to claim exclusive
/// ownership of a shard for the duration of a processing transaction, ensuring FIFO
/// message delivery within each shard.
/// </summary>
public sealed class OutboxLock
{
    /// <summary>Zero-based shard index.</summary>
    public int Shard { get; set; }
}
