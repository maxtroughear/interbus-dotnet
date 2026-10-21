namespace InterBus.EntityFrameworkCore.Tests;

/// <summary>
/// Consumer that publishes <see cref = "OrderShipped"/> to the outbox when it receives
/// <see cref = "OrderPlaced"/>. Saves the DbContext explicitly so the outbox row is committed.
/// </summary>
public sealed class OrderPlacedPublishingConsumer(TestDbContext dbContext, IPublisher publisher) : IConsumer<OrderPlaced>
{
    public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        await publisher.PublishAsync(new OrderShipped(message.OrderId), cancellationToken);
        await dbContext.SaveChangesAsync(cancellationToken);
    }
}
