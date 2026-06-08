using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.DynamoDB.Sagas;

public class DynamoDbSagaBuilder<TSaga>(IServiceCollection services)
{
    internal DynamoDbSagaOptions<TSaga> Options { get; } = new();

    public DynamoDbSagaBuilder<TSaga> TableName(string tableName)
    {
        Options.TableName = tableName;
        return this;
    }

    public DynamoDbSagaBuilder<TSaga> ShareOutboxTransaction()
    {
        services.Replace(ServiceDescriptor.Scoped<IDynamoDbStateWriter<TSaga>, DynamoDbOutboxStateWriter<TSaga>>());
        return this;
    }
}
