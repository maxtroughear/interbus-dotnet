using System.Collections.Concurrent;

namespace InterBus.PostgreSQL.Tests;

/// <summary>
/// Singleton-safe tracker registered in DI so all consumer scopes write to the same list.
/// </summary>
public class MessageTracker<T> where T : class
{
    private readonly ConcurrentBag<T> _messages = [];
    public IReadOnlyCollection<T> Messages => _messages;
    public void Track(T message) => _messages.Add(message);
}

public sealed class OrderPlacedTracker : MessageTracker<OrderPlaced> { }
public sealed class AuditTracker : MessageTracker<OrderPlaced> { }
public sealed class OrderShippedTracker : MessageTracker<OrderShipped> { }

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

public sealed class OrderShippedConsumer(OrderShippedTracker tracker) : IConsumer<OrderShipped>
{
    public Task Consume(OrderShipped message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Tracks whether the consumer received a non-null <see cref="PostgreSqlConsumeContext.Transaction"/>.
/// </summary>
public sealed class TransactionContextTracker
{
    public bool ReceivedTransaction { get; set; }
}

public sealed class TransactionAwareConsumer(
    OrderPlacedTracker tracker,
    TransactionContextTracker txTracker,
    PostgreSqlConsumeContext consumeCtx) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        txTracker.ReceivedTransaction = consumeCtx.Transaction is not null;
        return Task.CompletedTask;
    }
}
