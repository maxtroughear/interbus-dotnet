using System.Collections.Concurrent;

namespace InterBus.AzureServiceBus.Tests;

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

public sealed class OrderShippedTracker : MessageTracker<OrderShipped>;

public sealed class SessionOrderPlacedTracker : MessageTracker<SessionOrderPlaced>;

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

public sealed class SessionOrderPlacedConsumer(SessionOrderPlacedTracker tracker)
    : IConsumer<SessionOrderPlaced>
{
    public Task Consume(SessionOrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Consumer for request/reply tests. Reads the ReplyTo address from the consume context
/// and sends a reply back via the publisher.
/// </summary>
public sealed class GetOrderStatusConsumer(IPublisher publisher) : IConsumer<GetOrderStatusRequest>
{
    public async Task Consume(GetOrderStatusRequest message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        if (context.ReplyTo is null) return;
        await publisher.SendAsync(
            context.ReplyTo,
            new GetOrderStatusReply(message.OrderId, "Shipped"),
            cancellationToken);
    }
}
