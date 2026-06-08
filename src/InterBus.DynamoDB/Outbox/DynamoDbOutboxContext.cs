using System.IO.Hashing;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// DynamoDB-backed implementation of <see cref="IDynamoDbOutboxContext"/>.
/// </summary>
internal sealed class DynamoDbOutboxContext(
    IAmazonDynamoDB dynamoDb,
    DynamoDbOutboxOptions options,
    DynamoDbOutboxConsumeState consumeState)
    : IDynamoDbOutboxContext
{
    // -----------------------------------------------------------------------
    // DynamoDB attribute name constants — exposed internally so that the
    // processor and inline-delivery middleware can reference the same names.
    // -----------------------------------------------------------------------

    // Delegate to public DynamoDbOutboxTable constants so internal code and tests use the same values.
    internal const string AttrGroupId = DynamoDbOutboxTable.GroupId;
    internal const string AttrEntryId = DynamoDbOutboxTable.EntryId;
    internal const string AttrShard = DynamoDbOutboxTable.Shard;
    internal const string AttrVisibleAt = DynamoDbOutboxTable.VisibleAt;
    internal const string AttrCreatedAt = DynamoDbOutboxTable.CreatedAt;
    internal const string AttrDestination = DynamoDbOutboxTable.Destination;
    internal const string AttrPayload = DynamoDbOutboxTable.Payload;
    internal const string AttrHeadersJson = DynamoDbOutboxTable.HeadersJson;
    internal const string AttrDelay = DynamoDbOutboxTable.Delay;
    internal const string GroupEntryId = DynamoDbOutboxTable.GroupEntryValue;

    // -----------------------------------------------------------------------
    // Instance state
    // -----------------------------------------------------------------------

    private readonly List<TransactWriteItem> _businessItems = [];
    private readonly List<PendingOutboxMessage> _outboxMessages = [];
    private Guid? _groupId;
    internal bool HasPendingBusinessItems => _businessItems.Count > 0;
    internal bool HasPendingOutboxMessages => _outboxMessages.Count > 0;
    internal Guid? PendingGroupId => _groupId;

    // -----------------------------------------------------------------------
    // IDynamoDbOutboxContext
    // -----------------------------------------------------------------------

    public void AddTransactItem(TransactWriteItem item)
        => _businessItems.Add(item);

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_outboxMessages.Count == 0 && _businessItems.Count == 0)
            return;

        var items = new List<TransactWriteItem>(_businessItems);

        if (_outboxMessages.Count > 0)
        {
            var groupIdGuid = _groupId!.Value;
            var groupId = groupIdGuid.ToString("N");
            var now = DateTimeOffset.UtcNow;
            var createdAtEpoch = now.ToUnixTimeSeconds().ToString();
            var visibleAtEpoch = (consumeState.IsDeliveryInline
                ? now.Add(options.ConsumerVisibilityTimeout)
                : now).ToUnixTimeSeconds().ToString();

            // Assign the group to a shard deterministically by hashing the group ID.
            var shardBytes = groupIdGuid.ToByteArray();
            var shardIndex = XxHash3.HashToUInt64(shardBytes) % (ulong)options.ShardCount;

            // Group header item
            items.Add(new TransactWriteItem
            {
                Put = new Put
                {
                    TableName = options.TableName,
                    Item = new Dictionary<string, AttributeValue>
                    {
                        [AttrGroupId] = new() { S = groupId },
                        [AttrEntryId] = new() { S = GroupEntryId },
                        [AttrShard] = new() { N = shardIndex.ToString() },
                        [AttrVisibleAt] = new() { N = visibleAtEpoch },
                        [AttrCreatedAt] = new() { N = createdAtEpoch },
                    },
                },
            });

            // Message items
            foreach (var msg in _outboxMessages)
            {
                var msgItem = new Dictionary<string, AttributeValue>
                {
                    [AttrGroupId] = new() { S = groupId },
                    [AttrEntryId] = new() { S = msg.Id.ToString("N") },
                    [AttrDestination] = new() { S = msg.Destination },
                    [AttrPayload] = new() { B = new MemoryStream(msg.Payload) },
                    [AttrHeadersJson] = new() { S = msg.HeadersJson },
                    [AttrCreatedAt] = new() { N = createdAtEpoch },
                };

                if (msg.Delay.HasValue)
                    msgItem[AttrDelay] = new AttributeValue
                        { N = ((long)msg.Delay.Value.TotalSeconds).ToString() };

                items.Add(new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = options.TableName,
                        Item = msgItem,
                    },
                });
            }

            if (consumeState.IsDeliveryInline)
                consumeState.GroupId = _groupId.Value;
        }

        await dynamoDb.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = items,
        }, cancellationToken);

        _businessItems.Clear();
        _outboxMessages.Clear();
        _groupId = null;
    }

    // -----------------------------------------------------------------------
    // Internal — called by DynamoDbOutboxPublishMiddleware
    // -----------------------------------------------------------------------

    internal void AddOutboxMessage(PendingOutboxMessage message)
    {
        _groupId ??= Guid.CreateVersion7();
        _outboxMessages.Add(message);
    }

    // -----------------------------------------------------------------------
    // Helper: header deserialization (used by processor and inline delivery)
    // -----------------------------------------------------------------------

    internal static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize(json, DynamoDbOutboxJsonContext.Default.DictionaryStringString)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }

    internal static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, DynamoDbOutboxJsonContext.Default.DictionaryStringString);
}

/// <summary>
/// In-memory representation of a publish call that has been captured by
/// <see cref="DynamoDbOutboxPublishMiddleware"/> but not yet committed.
/// </summary>
internal sealed record PendingOutboxMessage(
    Guid Id,
    string Destination,
    byte[] Payload,
    string HeadersJson,
    TimeSpan? Delay);
