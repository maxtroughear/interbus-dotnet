using System.Text.Json;
using InterBus.Consumer;
using InterBus.DeadLettering;
using InterBus.Publisher;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Internal;

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
/// paired <see cref="OutboxConsumeMiddleware{TDbContext}"/> (when registered) attempts immediate
/// inline delivery, deleting the group on success. If inline delivery fails the group remains in
/// the outbox and the processor retries it after <see cref="OutboxGroup.VisibleAt"/> has elapsed.
/// </para>
/// <para>
/// All messages published within the same DI scope share the same
/// <see cref="OutboxMessage.GroupId"/>, allowing the processor to fetch and forward them as a
/// single atomic group.
/// </para>
/// <para>
/// <b>Exception — <see cref="DeadLetter{T}"/> messages</b>: these are published by
/// <see cref="InterBus.DeadLettering.DeadLetterConsumeMiddleware"/> in the consume failure path,
/// where no <c>SaveChangesAsync</c> is ever called. Buffering them in the outbox would cause them
/// to be silently lost, so they are forwarded directly to the transport by calling
/// <paramref name="next"/> instead.
/// </para>
/// </summary>
internal sealed class OutboxPublishMiddleware<TDbContext>(
    TDbContext dbContext,
    IMessageSerializer serializer,
    OutboxOptions options,
    ConsumeContextAccessor contextAccessor,
    OutboxConsumeState consumeState)
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

        var isConsumerContext = contextAccessor.ConsumeContext is not null;

        // Bus context: VisibleAt = UtcNow (immediately eligible).
        // Consumer context: VisibleAt = UtcNow + ConsumerVisibilityTimeout (may equal UtcNow when
        // timeout is zero) - prevents the processor racing with inline delivery.
        var visibleAt = isConsumerContext
            ? DateTime.UtcNow + options.ConsumerVisibilityTimeout
            : DateTime.UtcNow;

        _outboxGroup = new OutboxGroup { Id = Guid.CreateVersion7(), VisibleAt = visibleAt };
        dbContext.Add(_outboxGroup);

        // Notify the paired OutboxConsumeMiddleware so it can attempt inline delivery
        // after the consumer's Consume handler (and its SaveChangesAsync) has finished.
        if (isConsumerContext) consumeState.GroupId = _outboxGroup.Id;

        return _outboxGroup;
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);
}
