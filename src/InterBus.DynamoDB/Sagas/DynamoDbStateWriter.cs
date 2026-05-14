using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace InterBus.DynamoDB.Sagas;

public class DynamoDbStateWriter<TSaga>(IAmazonDynamoDB dynamoDb) : IDynamoDbStateWriter<TSaga>
{
    public Task PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default)
    {
        return dynamoDb.PutItemAsync(request, cancellationToken);
    }
}
