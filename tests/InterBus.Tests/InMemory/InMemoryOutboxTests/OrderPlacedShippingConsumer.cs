namespace InterBus.Tests.InMemory;

public partial class InMemoryOutboxTests
{
    internal class OrderPlacedShippingConsumer(OrderPlacedTracker tracker, IPublisher publisher) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            await publisher.PublishAsync(new OrderShipped(message.OrderId, string.Empty), cancellationToken);
        }
    }
}
