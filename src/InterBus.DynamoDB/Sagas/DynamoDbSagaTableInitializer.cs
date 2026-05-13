using Amazon.DynamoDBv2;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.DynamoDB.Sagas;

/// <summary>
/// Creates the DynamoDB saga table on application startup when
/// <see cref="DynamoDbSagaOptions.CreateTableOnStartup"/> is <c>true</c>.
/// The operation is idempotent: if the table already exists it is left unchanged.
/// </summary>
internal sealed class DynamoDbSagaTableInitializer<TSaga>(
    IAmazonDynamoDB dynamoDb,
    DynamoDbSagaOptions<TSaga> options,
    ILogger<DynamoDbSagaTableInitializer<TSaga>> logger)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogDebug("Ensuring DynamoDB saga table '{TableName}' exists", options.TableName);
        await DynamoDbSagaTable.EnsureTableCreatedAsync(dynamoDb, options.TableName, cancellationToken);
        logger.LogDebug("DynamoDB saga table '{TableName}' is ready", options.TableName);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
