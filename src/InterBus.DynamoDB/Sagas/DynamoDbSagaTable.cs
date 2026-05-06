using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace InterBus.DynamoDB.Sagas;

/// <summary>
/// Defines the DynamoDB attribute names used by the InterBus saga table.
/// Reference these constants when creating the saga table or querying it directly.
/// <para>
/// The table must be created with partition key <see cref="CorrelationId"/> (String) and sort key
/// <see cref="SagaType"/> (String). Multiple saga types may share the same table because the
/// fully-qualified saga type name is stored as the sort key.
/// </para>
/// </summary>
public static class DynamoDbSagaTable
{
    /// <summary>Partition key attribute name (String).</summary>
    public const string CorrelationId = "CorrelationId";

    /// <summary>
    /// Sort key attribute name (String). Stores the fully-qualified name of the saga type,
    /// allowing multiple saga types to share the same DynamoDB table.
    /// </summary>
    public const string SagaType = "SagaType";

    /// <summary>
    /// Creates the DynamoDB saga table if it does not already exist, then waits until it is
    /// active. The table is created with <see cref="CorrelationId"/> as the partition key and
    /// <see cref="SagaType"/> as the sort key, using on-demand billing mode.
    /// If the table already exists this method returns immediately without making any changes.
    /// </summary>
    /// <param name="dynamoDb">The DynamoDB client.</param>
    /// <param name="tableName">The name of the table to create.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
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
            // Table does not exist — create it below.
        }

        await dynamoDb.CreateTableAsync(new CreateTableRequest
        {
            TableName = tableName,
            KeySchema =
            [
                new KeySchemaElement(CorrelationId, KeyType.HASH),
                new KeySchemaElement(SagaType, KeyType.RANGE),
            ],
            AttributeDefinitions =
            [
                new AttributeDefinition(CorrelationId, ScalarAttributeType.S),
                new AttributeDefinition(SagaType, ScalarAttributeType.S),
            ],
            BillingMode = BillingMode.PAY_PER_REQUEST,
        }, cancellationToken);

        await WaitForTableActiveAsync(dynamoDb, tableName, cancellationToken);
    }

    private static async Task WaitForTableActiveAsync(
        IAmazonDynamoDB dynamoDb,
        string tableName,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var desc = await dynamoDb.DescribeTableAsync(tableName, cancellationToken);
            if (desc.Table.TableStatus == TableStatus.ACTIVE)
                return;
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        }
    }
}
