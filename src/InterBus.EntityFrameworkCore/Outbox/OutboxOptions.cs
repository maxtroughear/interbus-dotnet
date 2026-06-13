namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Configuration options for the InterBus transactional outbox.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// How long each outbox worker waits before polling again when no claimable groups are found.
    /// Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of more frequent
    /// database queries when the outbox is empty.
    /// <para>
    /// The processor does not wait between iterations when messages are available — it
    /// immediately processes the next group after completing the current one.
    /// </para>
    /// </summary>
    public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Number of independent FIFO shards used by the outbox processor. Defaults to <c>128</c>.
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
    /// <para>
    /// Changing this value in production changes the hash partitioning of message groups.
    /// Existing and newly written groups may route to different shards immediately after rollout,
    /// which can shift throughput/fairness characteristics during mixed-version deployments.
    /// </para>
    /// </summary>
    public int ShardCount { get; set; } = 128;

    /// <summary>
    /// Maximum number of concurrent outbox workers per process.
    /// Each worker claims and processes one outbox group at a time in its own transaction.
    /// Defaults to <c>1</c>.
    /// </summary>
    public int MaxConcurrentWorkers { get; set; } = 1;

    public bool RunOutboxProcessor { get; internal set; } = true;
}
