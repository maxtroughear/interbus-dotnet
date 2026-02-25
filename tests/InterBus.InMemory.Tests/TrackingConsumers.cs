using System.Collections.Concurrent;

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
public sealed class OrderPlacedTracker : MessageTracker<OrderPlaced>
{
}

public sealed class AuditTracker : MessageTracker<OrderPlaced>
{
}

public sealed class OrderShippedTracker : MessageTracker<OrderShipped>
{
}

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
