using InterBus.Tests.Common;

namespace InterBus.DynamoDB.Tests;

public sealed class OutboxInboxFailFirstConsumer(OutboxConsumerExecutionTracker executionTracker, FailFirstConsumeState state) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        executionTracker.Track();
        if (Interlocked.Increment(ref state.CallCount) == 1)
            throw new Exception("Simulated consumer failure");
        return Task.CompletedTask;
    }
}
