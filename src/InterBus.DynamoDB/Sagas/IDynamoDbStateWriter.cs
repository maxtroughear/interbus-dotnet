using Amazon.DynamoDBv2.Model;

namespace InterBus.DynamoDB.Sagas;

// ReSharper disable once UnusedTypeParameter
public interface IDynamoDbStateWriter<TSaga>
{
    Task PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default);
}