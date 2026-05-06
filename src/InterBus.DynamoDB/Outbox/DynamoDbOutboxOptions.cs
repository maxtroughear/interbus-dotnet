namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Configuration options for the InterBus DynamoDB transactional outbox.
/// </summary>
public sealed class DynamoDbOutboxOptions
{
    /// <summary>
    /// Name of the DynamoDB table used to store outbox groups and messages.
    /// <para>
    /// The table must be created with a partition key named <c>GroupId</c> (String) and
    /// a sort key named <c>EntryId</c> (String) before the outbox can be used.
    /// </para>
    /// Defaults to <c>"InterBusOutbox"</c>.
    /// </summary>
    public string TableName { get; set; } = "InterBusOutbox";

    /// <summary>
    /// How often the background processor polls the outbox table when there are no pending
    /// messages. Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of
    /// more frequent DynamoDB reads when the outbox is empty.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long after a group is created by a consumer's consume handler before the outbox
    /// processor is allowed to pick it up. Only applies when inline delivery is configured
    /// via <c>UseOutboxWithInlineDelivery()</c>. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan ConsumerVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long the background processor holds a lock on a group item by pushing
    /// <c>VisibleAt</c> into the future. If the processor crashes while holding the lock,
    /// another instance can reclaim the group after this duration has elapsed.
    /// Defaults to 30 seconds.
    /// </summary>
    public TimeSpan ProcessorLockDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Number of shards used to distribute outbox groups across the GSI partition space.
    /// Each group header is assigned to a shard between 0 (inclusive) and
    /// <see cref="ShardCount"/> (exclusive) at creation time.
    /// <para>
    /// Higher values allow more processor instances to run in parallel (one per shard)
    /// without contention. Requires a GSI — see <see cref="GsiIndexName"/>. Defaults to 1.
    /// </para>
    /// </summary>
    public int ShardCount { get; set; } = 1;

    /// <summary>
    /// Name of the Global Secondary Index (GSI) to query for eligible groups instead of
    /// performing a full-table scan. The GSI must have <c>Shard</c> (Number) as the partition
    /// key and <c>VisibleAt</c> (Number) as the sort key. Set to <c>null</c> to fall back to a
    /// full-table scan (suitable for low-throughput scenarios). Defaults to
    /// <c>"InterBusOutbox-VisibleAt-index"</c>.
    /// </summary>
    public string? GsiIndexName { get; set; } = "InterBusOutbox-VisibleAt-index";

    internal bool RunOutboxProcessor { get; set; } = true;
}
