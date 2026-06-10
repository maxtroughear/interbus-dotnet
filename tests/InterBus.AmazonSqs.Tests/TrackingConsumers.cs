using System.Collections.Concurrent;

namespace InterBus.AmazonSqs.Tests;

/// <summary>
/// Singleton-safe tracker registered in DI so all consumer scopes write to the same list.
/// </summary>
public class MessageTracker<T> where T : class
{
    private readonly ConcurrentBag<T> _messages = [];
    public IReadOnlyCollection<T> Messages => _messages;
    public void Track(T message) => _messages.Add(message);
}

public sealed class OrderPlacedTracker : MessageTracker<OrderPlaced>;

public sealed class AuditTracker : MessageTracker<OrderPlaced>;

public abstract class OrderShippedTracker : MessageTracker<OrderShipped>;

public sealed class FifoOrderPlacedTracker : MessageTracker<FifoOrderPlaced>;

public sealed class FifoOrderDirectTracker : MessageTracker<FifoOrderDirect>;

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

public sealed class FifoOrderPlacedConsumer(FifoOrderPlacedTracker tracker) : IConsumer<FifoOrderPlaced>
{
    public Task Consume(FifoOrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

public sealed class FifoOrderDirectConsumer(FifoOrderDirectTracker tracker) : IConsumer<FifoOrderDirect>
{
    public Task Consume(FifoOrderDirect message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}
