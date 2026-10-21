namespace InterBus.Tests.InMemory;

public partial class DeadLetterConsumerTests
{
    /// <summary>Consumer that always throws, causing the message to be dead-lettered.</summary>
    public sealed class FaultingOrderPlacedConsumer(FaultingConsumerCallCount count) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            count.Increment();
            throw new InvalidOperationException("Intentional failure for dead-letter test");
        }
    }
}
