namespace InterBus.Tests.InMemory;

public sealed class RedeliveryTestConsumer(RedeliveryTracker tracker) : IConsumer<RedeliveryTestMessage>
{
    public Task Consume(RedeliveryTestMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        var attempt = tracker.RecordAttempt(message.Id);
        tracker.RecordRetryHeader(context.Headers);
        if (attempt == 1)
            throw new InvalidOperationException("fail first attempt");
        tracker.RecordSuccess(message.Id);
        return Task.CompletedTask;
    }
}
