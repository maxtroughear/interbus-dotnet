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
    /// Name of the DynamoDB table used by the inbox+outbox consumer flow to track per-consumer
    /// message processing state across redeliveries.
    /// <para>
    /// The table must be created with partition key <c>ConsumerName</c> (String) and sort key
    /// <c>MessageId</c> (String). Defaults to <c>"InterBusInbox"</c>.
    /// </para>
    /// </summary>
    public string InboxTableName { get; set; } = "InterBusInbox";

    /// <summary>
    /// How often the background processor polls the outbox table when there are no pending
    /// messages. Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of
    /// more frequent DynamoDB reads when the outbox is empty.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long after a group is created by a consumer's consume handler before the outbox
    /// processor is allowed to pick it up. Only applies when inline delivery is configured
    /// via <c>UseDynamoDbOutboxWithInlineDelivery()</c>. Defaults to 5 seconds.
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
    /// Each group header is assigned to a shard deterministically via a hash of its group ID
    /// modulo <see cref="ShardCount"/>.
    /// <para>
    /// Higher values allow more processor instances to run in parallel (one per shard)
    /// without contention. Requires a GSI — see <see cref="GsiIndexName"/>. Defaults to 1.
    /// </para>
    /// </summary>
    public int ShardCount { get; set; } = 1;

    /// <summary>
    /// Number of shards the background processor monitors. When <c>null</c> (the default),
    /// the processor uses <see cref="ShardCount"/>. Set this to a higher value than
    /// <see cref="ShardCount"/> to drain messages that were written before a shard-count
    /// reduction — the processor will keep polling old shard numbers until they are empty,
    /// then you can lower this value back to <see cref="ShardCount"/>.
    /// </summary>
    public int? ProcessorShardCount { get; set; }

    /// <summary>
    /// Name of the Global Secondary Index (GSI) used to query eligible groups by shard and
    /// <c>VisibleAt</c>. The GSI must have <c>Shard</c> (Number) as the partition key and
    /// <c>VisibleAt</c> (Number) as the sort key. Defaults to
    /// <c>"InterBusOutbox-VisibleAt-index"</c>.
    /// </summary>
    public string GsiIndexName { get; set; } = "InterBusOutbox-VisibleAt-index";

    /// <summary>
    /// Maximum number of candidates fetched from the GSI per shard per poll iteration.
    /// When multiple processors compete for the same shard, fetching more candidates allows
    /// the processor to try the next item immediately rather than making an additional
    /// round trip. Defaults to 5.
    /// </summary>
    public int ProcessorQueryBatchSize { get; set; } = 5;

    /// <summary>
    /// When <c>true</c>, the background <see cref="DynamoDbOutboxProcessor"/> hosted
    /// service is registered. Set to <c>false</c> when the processor runs in a separate
    /// process or when tests need direct control over message forwarding. Defaults to
    /// <c>true</c>.
    /// </summary>
    public bool RunOutboxProcessor { get; set; } = true;

    internal int EffectiveProcessorShardCount => ProcessorShardCount ?? ShardCount;

    /// <summary>
    /// When <c>true</c>, the DynamoDB outbox table (and its GSI when
    /// <see cref="GsiIndexName"/> is set) are automatically created on application startup
    /// if they do not already exist. Defaults to <c>false</c>.
    /// <para>
    /// The table is created with partition key <c>GroupId</c> (String) and sort key
    /// <c>EntryId</c> (String), using on-demand billing mode.
    /// </para>
    /// <para>
    /// <b>This option is intended for development and testing only.</b> For production
    /// environments, prefer managing DynamoDB tables with an Infrastructure-as-Code tool
    /// such as Terraform, AWS CloudFormation, or AWS CDK.
    /// </para>
    /// </summary>
    public bool CreateTableOnStartup { get; set; }

    /// <summary>
    /// When <c>true</c>, the DynamoDB inbox table used by
    /// <c>UseDynamoDbInboxOutbox()</c> is automatically created on startup if missing.
    /// Defaults to <c>false</c>.
    /// </summary>
    public bool CreateInboxTableOnStartup { get; set; }
}
