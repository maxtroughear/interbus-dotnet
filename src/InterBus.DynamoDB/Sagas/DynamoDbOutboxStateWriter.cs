using Amazon.DynamoDBv2.Model;
using InterBus.DynamoDB.Outbox;

namespace InterBus.DynamoDB.Sagas;

public class DynamoDbOutboxStateWriter<TSaga>(IDynamoDbOutboxContext outboxContext) : IDynamoDbStateWriter<TSaga>
{
    public async Task PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default)
    {
        outboxContext.AddTransactItem(new TransactWriteItem
        {
            Put = new Put
            {
                TableName = request.TableName,
                Item = request.Item,
                ConditionExpression = request.ConditionExpression,
                ExpressionAttributeNames = request.ExpressionAttributeNames,
                ExpressionAttributeValues = request.ExpressionAttributeValues
            }
        });

        await outboxContext.CommitAsync(cancellationToken);
    }
}
