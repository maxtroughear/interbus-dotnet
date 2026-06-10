namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Configuration options for the InterBus transactional outbox.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// How often the background processor polls the outbox table when there are no pending
    /// messages. Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of
    /// more frequent database queries when the outbox is empty.
    /// <para>
    /// The processor does not wait between iterations when messages are available — it
    /// immediately processes the next group after completing the current one.
    /// </para>
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Number of independent FIFO shards used by the outbox processor. Defaults to <c>1</c>.
    /// <para>
    /// With a single shard the outbox delivers messages in strict global FIFO order: only one
    /// processor can hold the shard at a time, so groups are always forwarded oldest-first.
    /// </para>
    /// <para>
    /// Increasing the shard count allows multiple processor instances to work in parallel
    /// (higher throughput), but limits the ordering guarantee to <em>per-shard</em> FIFO. Use a
    /// value greater than 1 only when global ordering is not required.
    /// </para>
    /// <para>
    /// Shard locking requires database-level row locking and is only effective when using
    /// <c>UsePostgreSql()</c> or <c>UseSqlServer()</c>. The default EF Core provider ignores
    /// shards and is safe for single-instance deployments only.
    /// </para>
    /// </summary>
    public int ShardCount { get; set; } = 1;

    public bool RunOutboxProcessor { get; internal set; } = true;
}
