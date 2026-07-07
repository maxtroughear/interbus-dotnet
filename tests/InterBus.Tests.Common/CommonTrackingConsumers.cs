using System.Collections.Concurrent;

namespace InterBus.Tests.Common;

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

public abstract class TrackingConsumer<TMessage, TTracker>(TTracker tracker) : IConsumer<TMessage>
    where TMessage : class
    where TTracker : MessageTracker<TMessage>
{
    public Task Consume(TMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Consumer for request/reply tests. Reads the ReplyTo address from the consume context
/// and sends a reply back via the publisher.
/// </summary>
public abstract class RequestReplyConsumer<TRequest, TReply>(IPublisher publisher) : IConsumer<TRequest>
    where TRequest : class
    where TReply : class
{
    public async Task Consume(TRequest message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        if (context.ReplyTo is null)
            return;

        await publisher.SendAsync(
            context.ReplyTo,
            CreateReply(message),
            cancellationToken);
    }

    protected abstract TReply CreateReply(TRequest message);
}
