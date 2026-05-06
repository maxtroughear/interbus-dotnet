using InterBus.DeadLettering;
using InterBus.Publishing;

namespace InterBus.DynamoDB.Outbox.Internal;

/// <summary>
/// Publish middleware that captures every outgoing message and registers it with the
/// <see cref="DynamoDbOutboxContext"/> instead of forwarding it to the transport directly.
/// The message is only sent once the caller invokes
/// <see cref="IDynamoDbOutboxContext.CommitAsync"/>, which writes the outbox items together
/// with any application-provided <c>TransactWriteItem</c>s atomically via DynamoDB
/// <c>TransactWriteItems</c>.
/// </summary>
internal sealed class DynamoDbOutboxPublishMiddleware(
    DynamoDbOutboxContext outboxContext,
    IMessageSerializer serializer)
    : IPublishMiddleware
{
    public Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        // DeadLetter<> messages are published in the consume failure path where CommitAsync
        // is never called. Forward them directly to the transport to avoid silent loss.
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(DeadLetter<>))
            return next(context, cancellationToken);

        var payload = serializer.Serialize(context.Message);
        var headersJson = DynamoDbOutboxContext.SerializeHeaders(context.Headers);

        outboxContext.AddOutboxMessage(new PendingOutboxMessage(
            Id: Guid.CreateVersion7(),
            Destination: context.Destination.ToString(),
            Payload: payload,
            HeadersJson: headersJson,
            Delay: context.Delay));

        // Do not call next — the message will be forwarded by the processor (or inline
        // delivery) after the caller commits.
        return Task.CompletedTask;
    }
}
