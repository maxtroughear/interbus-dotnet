namespace InterBus.Tests.InMemory;

public sealed class OrderAcknowledgedConsumer(OrderAcknowledgedTracker tracker) : IConsumer<OrderAcknowledged>
{
    public Task Consume(OrderAcknowledged message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
