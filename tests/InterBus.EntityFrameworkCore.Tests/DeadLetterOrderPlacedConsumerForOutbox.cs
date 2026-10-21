using InterBus.DeadLettering;

namespace InterBus.EntityFrameworkCore.Tests;

/// <summary>Consumer of <see cref = "DeadLetter{OrderPlaced}"/> used by the outbox dead-letter test.</summary>
public sealed class DeadLetterOrderPlacedConsumerForOutbox(DeadLetterOrderPlacedTrackerForOutbox tracker) : IConsumer<DeadLetter<OrderPlaced>>
{
    public Task Consume(DeadLetter<OrderPlaced> message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
