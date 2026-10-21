namespace InterBus.AmazonSqs.Tests;

public partial class DeadLetteringTests
{
    public sealed class DeadLetterOrderPlacedConsumer(ReceiveTracker receiveTracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            receiveTracker.Increment();
            throw new InvalidOperationException("Simulated dead-letter failure");
        }
    }
}
