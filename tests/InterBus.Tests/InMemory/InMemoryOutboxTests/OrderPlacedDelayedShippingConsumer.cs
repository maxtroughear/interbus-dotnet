namespace InterBus.Tests.InMemory;

public partial class InMemoryOutboxTests
{
    internal class OrderPlacedDelayedShippingConsumer(OrderPlacedTracker tracker, DelayedConsumerController controller, IPublisher publisher) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            await publisher.PublishAsync(new OrderShipped(message.OrderId, string.Empty), cancellationToken);
            tracker.Track(message);
            controller.Published = true;
            await controller.Semaphore.WaitAsync(cancellationToken);
            controller.Semaphore.Release();
            if (controller.ShouldFail)
            {
                controller.ShouldFail = false;
                throw new Exception("Simulated failure");
            }
        }
    }
}
