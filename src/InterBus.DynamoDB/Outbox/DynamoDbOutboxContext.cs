using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// DynamoDB-backed implementation of <see cref="IDynamoDbOutboxContext"/>.
/// </summary>
internal sealed class DynamoDbOutboxContext(IAmazonDynamoDB dynamoDb) : IDynamoDbOutboxContext
{
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

    private readonly List<TransactWriteItem> _items = [];

    public void AddTransactItem(TransactWriteItem item)
        => _items.Add(item);

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_items.Count == 0)
            return;

        await dynamoDb.TransactWriteItemsAsync(new TransactWriteItemsRequest
        {
            TransactItems = _items,
        }, cancellationToken);

        _items.Clear();
    }

    internal static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize(json, DynamoDbOutboxJsonContext.Default.DictionaryStringString)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }

    internal static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, DynamoDbOutboxJsonContext.Default.DictionaryStringString);
}
