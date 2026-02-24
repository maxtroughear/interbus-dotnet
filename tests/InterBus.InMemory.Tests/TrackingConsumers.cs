using System.Collections.Concurrent;

namespace InterBus.InMemory.Tests;

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
public sealed class OrderPlacedTracker : MessageTracker<OrderPlaced> { }
public sealed class AuditTracker : MessageTracker<OrderPlaced> { }

public sealed class OrderPlacedConsumer : IConsumer<OrderPlaced>
{
    private readonly OrderPlacedTracker _tracker;
    public OrderPlacedConsumer(OrderPlacedTracker tracker) => _tracker = tracker;

    public Task Consume(OrderPlaced message, CancellationToken cancellationToken = default)
    {
        _tracker.Track(message);
        return Task.CompletedTask;
    }
}

public sealed class AuditConsumer : IConsumer<OrderPlaced>
{
    private readonly AuditTracker _tracker;
    public AuditConsumer(AuditTracker tracker) => _tracker = tracker;

    public Task Consume(OrderPlaced message, CancellationToken cancellationToken = default)
    {
        _tracker.Track(message);
        return Task.CompletedTask;
    }
}
