namespace InterBus.PostgreSQL.Tests;

public partial class DeadLetteringTests
{
    public sealed class DeadLetterOrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            throw new InvalidOperationException("Simulated dead-letter failure");
        }
    }
}
