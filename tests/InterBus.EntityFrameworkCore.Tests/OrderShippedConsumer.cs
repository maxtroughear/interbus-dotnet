namespace InterBus.EntityFrameworkCore.Tests;

public sealed class OrderShippedConsumer(OrderShippedTracker tracker) : IConsumer<OrderShipped>
{
    public Task Consume(OrderShipped message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
