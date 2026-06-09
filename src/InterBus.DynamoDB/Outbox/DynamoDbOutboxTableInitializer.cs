using Amazon.DynamoDBv2;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Creates the DynamoDB outbox table (and its GSI when configured) on application startup
/// when <see cref="DynamoDbOutboxOptions.CreateTablesOnStartup"/> is <c>true</c>.
/// The operation is idempotent: if the table already exists it is left unchanged.
/// </summary>
internal sealed class DynamoDbOutboxTableInitializer(
    IAmazonDynamoDB dynamoDb,
    DynamoDbOutboxOptions options,
    ILogger<DynamoDbOutboxTableInitializer> logger)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogDebug("Ensuring DynamoDB outbox table '{TableName}' exists", options.TableName);
        await DynamoDbOutboxTable.EnsureTableCreatedAsync(
            dynamoDb, options.TableName, options.GsiIndexName, cancellationToken);
        logger.LogDebug("DynamoDB outbox table '{TableName}' is ready", options.TableName);

        logger.LogDebug("Ensuring DynamoDB inbox table '{TableName}' exists", options.InboxTableName);
        await DynamoDbInboxTable.EnsureTableCreatedAsync(
            dynamoDb, options.InboxTableName, cancellationToken);
        logger.LogDebug("DynamoDB inbox table '{TableName}' is ready", options.InboxTableName);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
