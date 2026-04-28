using System.Collections.Concurrent;
using InterBus.DeadLettering;

namespace InterBus.InMemory.Tests;

// ReSharper disable ClassNeverInstantiated.Global
/// <summary>
/// Singleton-safe tracker. Registered as singleton in DI so all consumer scopes write to the same list.
/// Use distinct subclasses to differentiate trackers for different consumers handling the same message type.
/// </summary>
public class MessageTracker<T> where T : class
{
    private readonly ConcurrentBag<T> _messages = [];
    public IReadOnlyCollection<T> Messages => _messages;
    public void Track(T message) => _messages.Add(message);
}

// Distinct tracker types so OrderPlacedConsumer and AuditConsumer don't share the same singleton.
public sealed class OrderPlacedTracker : MessageTracker<OrderPlaced>;

public sealed class AuditTracker : MessageTracker<OrderPlaced>;

public sealed class OrderShippedTracker : MessageTracker<OrderShipped>;

/// <summary>
/// Distinct trackers for <see cref="MultiTypeConsumer"/> so it doesn't interfere with
/// <see cref="OrderPlacedConsumer"/> and <see cref="OrderShippedTracker"/> in other tests.
/// </summary>
public sealed class MultiTypeOrderPlacedTracker : MessageTracker<OrderPlaced>;

public sealed class MultiTypeOrderShippedTracker : MessageTracker<OrderShipped>;

/// <summary>Tracks dead-lettered <see cref="OrderPlaced"/> messages for integration tests.</summary>
public sealed class DeadLetterOrderPlacedTracker : MessageTracker<DeadLetter<OrderPlaced>>;

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

public sealed class AuditConsumer(AuditTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// A consumer that handles two distinct message types.
/// With the one-queue-per-consumer design it is assigned a single queue; messages of both
/// types arrive on that queue and are dispatched to the correct <c>Consume</c> overload using
/// the source-generated, pre-compiled dispatch delegates (no runtime reflection).
/// Uses its own dedicated tracker types so tests for this consumer are isolated from
/// tests that use <see cref="OrderPlacedConsumer"/> or <see cref="OrderShippedTracker"/>.
/// </summary>
public sealed class MultiTypeConsumer(
    MultiTypeOrderPlacedTracker orderTracker,
    MultiTypeOrderShippedTracker shippedTracker) : IConsumer<OrderPlaced>, IConsumer<OrderShipped>
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
