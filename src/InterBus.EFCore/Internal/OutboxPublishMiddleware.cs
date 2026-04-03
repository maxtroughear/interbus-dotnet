using System.Text.Json;
using InterBus.Middleware;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EFCore.Internal;

/// <summary>
/// Publish middleware that writes outgoing messages to the transactional outbox instead of
/// forwarding them to the transport directly. The <see cref="OutboxProcessor{TDbContext}"/>
/// background service later reads the outbox and forwards each message to the transport.
/// <para>
/// Because the message is added to the same <typeparamref name="TDbContext"/> that the
/// application uses for its own business-logic writes, both the business data and the outbox
/// row are committed atomically when the caller invokes
/// <c>dbContext.SaveChangesAsync()</c>. This guarantees that a message is only forwarded if
/// the business transaction committed, eliminating dual-write races.
/// </para>
/// </summary>
internal sealed class OutboxPublishMiddleware<TDbContext>(
    TDbContext dbContext,
    IMessageSerializer serializer)
    : IPublishMiddleware
    where TDbContext : DbContext, IOutboxDbContext
{
    public Task InvokeAsync<T>(
        PublishContext<T> context,
        Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var payload = serializer.Serialize(context.Payload);
        var headersJson = SerializeHeaders(context.Headers);
        var visibleAt = context.Delay.HasValue
            ? (DateTime?)DateTime.UtcNow.Add(context.Delay.Value)
            : null;

        dbContext.OutboxMessages.Add(new OutboxMessage
        {
            Destination = context.Destination.ToString(),
            Payload = payload,
            HeadersJson = headersJson,
            VisibleAt = visibleAt,
        });

        // Do not call next — the message will be sent by OutboxProcessor once the
        // surrounding database transaction has committed.
        return Task.CompletedTask;
    }

    internal static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);
}
