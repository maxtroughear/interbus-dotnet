namespace InterBus.EntityFrameworkCore.Tests;

public sealed class OrderPlacedPublishingTrackedConsumer(TestDbContext dbContext, IPublisher publisher, OrderPlacedPublishingTracker tracker) : IConsumer<OrderPlaced>
{
    public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message.OrderId);
        await publisher.PublishAsync(new OrderShipped(message.OrderId), cancellationToken);
        await dbContext.SaveChangesAsync(cancellationToken);
    }
}
