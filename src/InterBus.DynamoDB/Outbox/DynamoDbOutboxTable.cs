namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Defines the DynamoDB attribute names and key values used by the InterBus outbox table.
/// Reference these constants when creating the outbox table or querying it directly.
/// <para>
/// The table must be created with partition key <see cref="GroupId"/> (String) and sort key
/// <see cref="EntryId"/> (String).
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
    /// Unix epoch seconds when the group becomes eligible for processing. Number attribute.
    /// </summary>
    public const string VisibleAt = "VisibleAt";

    /// <summary>Unix epoch seconds when the item was created. Number attribute.</summary>
    public const string CreatedAt = "CreatedAt";

    /// <summary>
    /// UUID of the processor currently holding a lock on this group. String attribute (optional).
    /// </summary>
    public const string ProcessorId = "ProcessorId";

    /// <summary>
    /// Unix epoch seconds when the processor lock expires. Number attribute (optional).
    /// </summary>
    public const string ProcessorLockExpiresAt = "ProcessorLockExpiresAt";

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
