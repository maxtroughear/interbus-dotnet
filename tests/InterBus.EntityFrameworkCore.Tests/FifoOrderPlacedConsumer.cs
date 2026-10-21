namespace InterBus.EntityFrameworkCore.Tests;

public sealed class FifoOrderPlacedConsumer(FifoOrderIdTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message.OrderId);
        return Task.CompletedTask;
    }
}
