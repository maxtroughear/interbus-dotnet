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
    /// processor is allowed to pick it up. Defaults to 5 seconds.
    /// <para>
    /// Set to <see cref="TimeSpan.Zero"/> to make consumer-published groups immediately eligible
    /// for the background processor (useful when inline delivery is not configured).
    /// </para>
    /// </summary>
    public TimeSpan ConsumerVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long the background processor holds a lock on a group item.
    /// If the processor crashes while holding the lock, another instance can reclaim the
    /// group after this duration has elapsed. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan ProcessorLockDuration { get; set; } = TimeSpan.FromSeconds(30);

    internal bool RunOutboxProcessor { get; set; } = true;
}
