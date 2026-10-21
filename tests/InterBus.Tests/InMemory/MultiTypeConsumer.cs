namespace InterBus.Tests.InMemory;

/// <summary>
/// A consumer that handles two distinct message types.
/// With the one-queue-per-consumer design it is assigned a single queue; messages of both
/// types arrive on that queue and are dispatched to the correct <c>Consume</c> overload using
/// the source-generated, pre-compiled dispatch delegates (no runtime reflection).
/// Uses its own dedicated tracker types so tests for this consumer are isolated from
/// tests that use <see cref = "OrderPlacedConsumer"/> or <see cref = "OrderShippedTracker"/>.
/// </summary>
public sealed class MultiTypeConsumer(OrderPlacedTracker orderTracker, OrderShippedTracker shippedTracker) : IConsumer<OrderPlaced>, IConsumer<OrderShipped>
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
