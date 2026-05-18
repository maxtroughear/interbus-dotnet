using System.Text.Json;
using InterBus.DeadLettering;
using InterBus.Publishing;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Publish middleware that writes every outgoing message to the transactional outbox instead of
/// forwarding it to the transport directly. The message is only sent once the caller commits its
/// database transaction, guaranteeing atomicity with any surrounding business-logic writes.
/// <para>
/// <b>Bus context</b> (message published via <see cref="IPublisher"/>):
/// the message is written to the outbox table with <see cref="OutboxGroup.VisibleAt"/> set to
/// <c>UtcNow</c> (immediately eligible) and will be forwarded by the
/// <see cref="OutboxProcessor{TDbContext}"/> background service once the caller commits.
/// </para>
/// <para>
/// <b>Consumer context</b> (message published from within a consumer's <c>Consume</c> handler):
/// the message is written to the outbox table with a
/// <see cref="OutboxGroup.VisibleAt"/> timestamp of
/// <c>UtcNow + <see cref="OutboxOptions.ConsumerVisibilityTimeout"/></c>.
/// After the consumer's <c>Consume</c> handler completes (and its transaction has committed), the
/// paired <see cref="InboxOutboxConsumeMiddleware{TDbContext}"/> (when registered) attempts immediate
/// inline delivery, deleting the group on success. If inline delivery fails the group remains in
/// the outbox and the processor retries it after <see cref="OutboxGroup.VisibleAt"/> has elapsed.
/// </para>
/// <para>
/// All messages published within the same DI scope share the same
/// <see cref="OutboxMessage.GroupId"/>, allowing the processor to fetch and forward them as a
/// single atomic group.
/// </para>
/// </summary>
internal sealed class OutboxPublishMiddleware<TDbContext>(TDbContext dbContext, IMessageSerializer serializer)
    : IPublishMiddleware
    where TDbContext : DbContext
{
    private OutboxGroup? _outboxGroup;

    public Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        // DeadLetter<> messages are published in the consume failure path, where no
        // SaveChangesAsync is ever called. Buffering them in the outbox would cause them to be
        // silently lost, so forward directly to the transport instead.
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(DeadLetter<>))
            return next(context, cancellationToken);

        var payload = serializer.Serialize(context.Message);
        var headersJson = SerializeHeaders(context.Headers);

        EnsureGroupExists().Messages.Add(new OutboxMessage
        {
            GroupId = _outboxGroup!.Id,
            Destination = context.Destination.ToString(),
            Payload = payload,
            HeadersJson = headersJson,
            Delay = context.Delay,
        });

        // Do not call next — messages are forwarded by OutboxProcessor (or inline delivery in
        // OutboxConsumeMiddleware for consumer-context publishes).
        return Task.CompletedTask;
    }

    private OutboxGroup EnsureGroupExists()
    {
        if (_outboxGroup is not null)
            return _outboxGroup;

        var visibleAt = DateTime.UtcNow;

        _outboxGroup = new OutboxGroup { Id = Guid.CreateVersion7(), VisibleAt = visibleAt };
        dbContext.Add(_outboxGroup);

        return _outboxGroup;
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);
}
