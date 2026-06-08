using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Defines the DynamoDB attribute names and key values used by the InterBus inbox table.
/// </summary>
public static class DynamoDbInboxTable
{
    /// <summary>Partition key attribute name (String).</summary>
    public const string ConsumerName = "ConsumerName";

    /// <summary>Sort key attribute name (String).</summary>
    public const string MessageId = "MessageId";

    /// <summary>Processing state attribute name (Number).</summary>
    public const string State = "State";

    /// <summary>Outbox group identifier for pending outbox delivery (String).</summary>
    public const string GroupId = "GroupId";

    /// <summary>Unix epoch seconds when the inbox row was created (Number).</summary>
    public const string CreatedAt = "CreatedAt";

    /// <summary>Unix epoch seconds when consumer execution completed (Number).</summary>
    public const string ConsumedAt = "ConsumedAt";

    /// <summary>Unix epoch seconds when outbox delivery completed (Number).</summary>
    public const string DeliveredAt = "DeliveredAt";

    /// <summary>
    /// Creates the DynamoDB inbox table if it does not already exist, then waits until it is
    /// active. The table is created with <see cref="ConsumerName"/> as partition key and
    /// <see cref="MessageId"/> as sort key, using on-demand billing mode.
    /// </summary>
    public static async Task EnsureTableCreatedAsync(
        IAmazonDynamoDB dynamoDb,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await dynamoDb.DescribeTableAsync(tableName, cancellationToken);
            return;
        }
        catch (ResourceNotFoundException)
        {
        }

        await dynamoDb.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema =
            [
                new KeySchemaElement(ConsumerName, KeyType.HASH),
                new KeySchemaElement(MessageId, KeyType.RANGE),
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition(ConsumerName, ScalarAttributeType.S),
                new AttributeDefinition(MessageId, ScalarAttributeType.S),
            ],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, cancellationToken);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(60);
        while (!cancellationToken.IsCancellationRequested && DateTimeOffset.UtcNow < deadline)
        {
            var desc = await dynamoDb.DescribeTableAsync(tableName, cancellationToken);
            if (desc.Table.TableStatus == TableStatus.ACTIVE)
                return;
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new TimeoutException(
            $"DynamoDB table '{tableName}' did not become ACTIVE within the expected time.");
    }
}
