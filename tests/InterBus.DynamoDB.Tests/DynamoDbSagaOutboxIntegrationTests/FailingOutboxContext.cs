using Amazon.DynamoDBv2.Model;
using InterBus.DynamoDB.Outbox;

namespace InterBus.DynamoDB.Tests;

public partial class DynamoDbSagaOutboxIntegrationTests
{
    private class FailingOutboxContext : IDynamoDbOutboxContext
    {
        internal List<TransactWriteItem> Items { get; } = [];

        public void AddTransactItem(TransactWriteItem item)
        {
            Items.Add(item);
        }

        public Task CommitAsync(CancellationToken cancellationToken = default)
        {
            throw new TimeoutException("Outbox commit timed out");
        }
    }
}
