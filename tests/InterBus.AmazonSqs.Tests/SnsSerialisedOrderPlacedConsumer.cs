namespace InterBus.AmazonSqs.Tests;

public sealed class SnsSerialisedOrderPlacedConsumer(SnsSerialisedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message, new MessageHeaders(context.Headers));
        return Task.CompletedTask;
    }
}
