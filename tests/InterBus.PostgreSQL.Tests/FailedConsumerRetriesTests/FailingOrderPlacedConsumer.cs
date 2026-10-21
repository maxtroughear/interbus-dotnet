namespace InterBus.PostgreSQL.Tests;

public partial class FailedConsumerRetriesTests
{
    internal sealed class FailingOrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            throw new InvalidOperationException("Simulated consumer failure");
        }
    }
}
