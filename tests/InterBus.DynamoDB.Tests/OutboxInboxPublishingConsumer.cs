using InterBus.Tests.Common;

namespace InterBus.DynamoDB.Tests;

public sealed class OutboxInboxPublishingConsumer(OutboxConsumerExecutionTracker executionTracker, IPublisher publisher) : IConsumer<OrderPlaced>
{
    public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        executionTracker.Track();
        await publisher.PublishAsync(new OrderShipped(message.OrderId, string.Empty), cancellationToken);
    }
}
