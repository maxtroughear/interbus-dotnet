using InterBus.DeadLettering;

namespace InterBus.Tests.InMemory;

/// <summary>
/// Receives dead-lettered <see cref = "OrderPlaced"/> messages wrapped in <see cref = "DeadLetter{T}"/>.
/// Used by dead letter consumer integration tests.
/// </summary>
public sealed class DeadLetterOrderPlacedConsumer(DeadLetterOrderPlacedTracker tracker) : IConsumer<DeadLetter<OrderPlaced>>
{
    public Task Consume(DeadLetter<OrderPlaced> message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
