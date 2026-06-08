using System.Text.Json;
using InterBus.DeadLettering;
using InterBus.Publishing;

namespace InterBus.EntityFrameworkCore.Outbox;

internal sealed class InboxOutboxPublishMiddleware(
    IMessageSerializer serializer,
    OutboxConsumeState consumeState)
    : IPublishMiddleware
{
    public Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(DeadLetter<>))
            return next(context, cancellationToken);

        if (consumeState.Inbox is null)
        {
            throw new InvalidOperationException("Ef Core Inbox and Outbox not configured");
        }

        var payload = serializer.Serialize(context.Message);
        var headersJson = SerializeHeaders(context.Headers);

        consumeState.Inbox.Messages.Add(new OutboxMessage
        {
            InboxId = consumeState.Inbox.Id,
            Destination = context.Destination.ToString(),
            Payload = payload,
            HeadersJson = headersJson,
            Delay = context.Delay,
        });

        return Task.CompletedTask;
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);
}
