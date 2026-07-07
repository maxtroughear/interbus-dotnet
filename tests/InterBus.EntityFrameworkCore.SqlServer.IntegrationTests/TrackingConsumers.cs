namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker)
    : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);

public sealed class SequencedOrderConsumer(SequencedOrderTracker tracker) : IConsumer<SequencedOrder>
{
    public Task Consume(SequencedOrder message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
