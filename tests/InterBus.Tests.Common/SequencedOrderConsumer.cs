namespace InterBus.Tests.Common;

public sealed class SequencedOrderConsumer(SequencedOrderTracker tracker) : IConsumer<SequencedOrder>
{
    public Task Consume(SequencedOrder message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
