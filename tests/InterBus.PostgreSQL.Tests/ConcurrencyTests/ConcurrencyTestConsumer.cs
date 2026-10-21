namespace InterBus.PostgreSQL.Tests;

public partial class ConcurrencyTests
{
    public sealed class ConcurrencyTestConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            return Task.CompletedTask;
        }
    }
}
