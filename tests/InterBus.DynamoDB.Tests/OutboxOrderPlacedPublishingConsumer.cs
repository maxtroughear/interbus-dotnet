using InterBus.DynamoDB.Outbox;
using InterBus.Tests.Common;

namespace InterBus.DynamoDB.Tests;

public sealed class OutboxOrderPlacedPublishingConsumer(IDynamoDbOutboxContext outboxContext, IPublisher publisher) : IConsumer<OrderPlaced>
{
    public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        await publisher.PublishAsync(new OrderShipped(message.OrderId, string.Empty), cancellationToken);
        await outboxContext.CommitAsync(cancellationToken);
    }
}
