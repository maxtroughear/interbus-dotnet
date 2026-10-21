namespace InterBus.Tests.Common;

/// <summary>
/// Consumer for request/reply tests. Reads the ReplyTo address from the consume context
/// and sends a reply back via the publisher.
/// </summary>
public abstract class RequestReplyConsumer<TRequest, TReply>(IPublisher publisher) : IConsumer<TRequest> where TRequest : class where TReply : class
{
    public async Task Consume(TRequest message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        if (context.ReplyTo is null)
            return;
        await publisher.SendAsync(context.ReplyTo, CreateReply(message), cancellationToken);
    }

    protected abstract TReply CreateReply(TRequest message);
}
