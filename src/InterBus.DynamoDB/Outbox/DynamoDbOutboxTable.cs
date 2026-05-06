namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Defines the DynamoDB attribute names and key values used by the InterBus outbox table.
/// Reference these constants when creating the outbox table or querying it directly.
/// <para>
/// The table must be created with partition key <see cref="GroupId"/> (String) and sort key
/// <see cref="EntryId"/> (String).
/// </para>
/// <para>
/// To enable efficient sharded queries and avoid full-table scans, create a Global Secondary
/// Index (GSI) with partition key <see cref="Shard"/> (Number) and sort key
/// <see cref="VisibleAt"/> (Number). Set <see cref="DynamoDbOutboxOptions.GsiIndexName"/> to
/// the name of this index. The recommended index name is
/// <c>"InterBusOutbox-VisibleAt-index"</c>. Multiple processor instances can each handle a
/// dedicated shard for horizontal scaling.
/// </para>
/// </summary>
public static class DynamoDbOutboxTable
{
    /// <summary>Partition key attribute name (String).</summary>
    public const string GroupId = "GroupId";

    /// <summary>Sort key attribute name (String). Value is <see cref="GroupEntryValue"/> for
    /// group headers and a MessageId for message items.</summary>
    public const string EntryId = "EntryId";

    /// <summary>
    /// Sort key value used for group header items. Message items use a unique MessageId as their
    /// sort key value.
    /// </summary>
    public const string GroupEntryValue = "GROUP";

    /// <summary>
    /// Shard number assigned to group header items. Number attribute (0-based).
    /// Used as the partition key of the optional <see cref="DynamoDbOutboxOptions.GsiIndexName"/>
    /// GSI to allow efficient per-shard queries and horizontal scaling.
    /// </summary>
    public const string Shard = "Shard";

    /// <summary>
    /// Unix epoch seconds when the group becomes eligible for processing. Number attribute.
    /// Also serves as the processor lock: when a processor acquires a group it sets
    /// <c>VisibleAt</c> to <c>now + ProcessorLockDuration</c>, hiding it from other
    /// processors until the lock expires or the group is deleted.
    /// </summary>
    public const string VisibleAt = "VisibleAt";

    /// <summary>Unix epoch seconds when the item was created. Number attribute.</summary>
    public const string CreatedAt = "CreatedAt";

    /// <summary>Destination URI for this message. String attribute.</summary>
    public const string Destination = "Destination";

    /// <summary>Serialized message payload. Binary attribute.</summary>
    public const string Payload = "Payload";

    /// <summary>JSON-serialized message headers. String attribute.</summary>
    public const string HeadersJson = "HeadersJson";

    /// <summary>
    /// Optional message delivery delay in whole seconds. Number attribute (optional).
    /// </summary>
    public const string Delay = "Delay";
}
