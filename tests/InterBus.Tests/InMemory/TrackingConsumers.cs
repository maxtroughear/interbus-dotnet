using InterBus.DeadLettering;

namespace InterBus.Tests.InMemory;

/// <summary>Tracks dead-lettered <see cref="OrderPlaced"/> messages for integration tests.</summary>
public sealed class DeadLetterOrderPlacedTracker : MessageTracker<DeadLetter<OrderPlaced>>;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker)
    : TrackingConsumer<OrderPlaced, OrderPlacedTracker>(tracker);

public sealed class AuditConsumer(AuditTracker tracker)
    : TrackingConsumer<OrderPlaced, AuditTracker>(tracker);

public sealed class OrderShippedConsumer(OrderShippedTracker tracker)
    : TrackingConsumer<OrderShipped, OrderShippedTracker>(tracker);

/// <summary>
/// A consumer that handles two distinct message types.
/// With the one-queue-per-consumer design it is assigned a single queue; messages of both
/// types arrive on that queue and are dispatched to the correct <c>Consume</c> overload using
/// the source-generated, pre-compiled dispatch delegates (no runtime reflection).
/// Uses its own dedicated tracker types so tests for this consumer are isolated from
/// tests that use <see cref="OrderPlacedConsumer"/> or <see cref="OrderShippedTracker"/>.
/// </summary>
public sealed class MultiTypeConsumer(
    OrderPlacedTracker orderTracker,
    OrderShippedTracker shippedTracker) : IConsumer<OrderPlaced>, IConsumer<OrderShipped>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        orderTracker.Track(message);
        return Task.CompletedTask;
    }

    public Task Consume(OrderShipped message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        shippedTracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Receives dead-lettered <see cref="OrderPlaced"/> messages wrapped in <see cref="DeadLetter{T}"/>.
/// Used by dead letter consumer integration tests.
/// </summary>
public sealed class DeadLetterOrderPlacedConsumer(DeadLetterOrderPlacedTracker tracker)
    : IConsumer<DeadLetter<OrderPlaced>>
{
    public Task Consume(DeadLetter<OrderPlaced> message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
