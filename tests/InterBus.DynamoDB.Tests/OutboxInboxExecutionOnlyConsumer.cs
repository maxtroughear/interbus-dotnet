using InterBus.Tests.Common;

namespace InterBus.DynamoDB.Tests;

public sealed class OutboxInboxExecutionOnlyConsumer(OutboxConsumerExecutionTracker executionTracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        executionTracker.Track();
        return Task.CompletedTask;
    }
}
