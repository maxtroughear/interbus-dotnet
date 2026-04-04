using System.Text.Json;
using InterBus.Middleware;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Internal;

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
/// <para>
/// All messages published within the same DI scope share the same
/// <see cref="OutboxMessage.GroupId"/>, allowing the background processor to fetch and
/// forward them as a single atomic group.
/// </para>
/// </summary>
internal sealed class OutboxPublishMiddleware<TDbContext>(
    TDbContext dbContext,
    IMessageSerializer serializer)
    : IPublishMiddleware
    where TDbContext : DbContext, IOutboxDbContext
{
    /// <summary>Shared by all Publish/Send calls within this DI scope.</summary>
    private readonly Guid _groupId = Guid.CreateVersion7();
    private OutboxGroup? _outboxGroup;

    public Task InvokeAsync<T>(
        PublishContext<T> context,
        Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var payload = serializer.Serialize(context.Payload);
        var headersJson = SerializeHeaders(context.Headers);

        EnsureGroupExists().Messages.Add(new OutboxMessage
        {
            GroupId = _groupId,
            Destination = context.Destination.ToString(),
            Payload = payload,
            HeadersJson = headersJson,
            Delay = context.Delay,
        });

        // Do not call next — the message will be sent by OutboxProcessor once the
        // surrounding database transaction has committed.
        return Task.CompletedTask;
    }

    private OutboxGroup EnsureGroupExists()
    {
        if (_outboxGroup is not null)
            return _outboxGroup;

        _outboxGroup = new OutboxGroup { Id = _groupId };
        dbContext.OutboxGroups.Add(_outboxGroup);
        return _outboxGroup;
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);
}
