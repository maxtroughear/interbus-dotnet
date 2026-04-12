using System.Text.Json;
using InterBus.Middleware;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Internal;

/// <summary>
/// Publish middleware that writes every outgoing message to the transactional outbox instead of
/// forwarding it to the transport directly. The message is only sent once the caller commits its
/// database transaction, guaranteeing atomicity with any surrounding business-logic writes.
/// <para>
/// <b>Bus context</b> (message published via <see cref="InterBus.IMessagePublisher"/>):
/// the message is written to the outbox table and will be forwarded by the
/// <see cref="OutboxProcessor{TDbContext}"/> background service once the caller commits.
/// </para>
/// <para>
/// <b>Consumer context</b> (message published from within a consumer's <c>Consume</c> handler):
/// the message is written to the outbox table with a
/// <see cref="OutboxGroup.VisibleAt"/> timestamp of
/// <c>UtcNow + <see cref="OutboxOptions.ConsumerVisibilityTimeout"/></c>.
/// After the consumer's <c>Consume</c> handler completes (and its transaction has committed), the
/// paired <see cref="OutboxConsumeMiddleware{TDbContext}"/> attempts immediate inline delivery,
/// deleting the group on success. If inline delivery fails the group remains in the outbox and
/// the processor retries it after <see cref="OutboxGroup.VisibleAt"/> has elapsed.
/// </para>
/// <para>
/// All messages published within the same DI scope share the same
/// <see cref="OutboxMessage.GroupId"/>, allowing the processor to fetch and forward them as a
/// single atomic group.
/// </para>
/// </summary>
internal sealed class OutboxSendMiddleware<TDbContext>(
    TDbContext dbContext,
    IMessageSerializer serializer,
    OutboxOptions options,
    IConsumeContextAccessor contextAccessor,
    OutboxConsumeState consumeState)
    : IPublishMiddleware
    where TDbContext : DbContext, IOutboxDbContext
{
    /// <summary>Shared by all Publish/Send calls within this DI scope.</summary>
    private readonly Guid _groupId = Guid.CreateVersion7();

    private OutboxGroup? _outboxGroup;

    public Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        var payload = serializer.Serialize(context.Message);
        var headersJson = SerializeHeaders(context.Headers);

        bool isConsumerContext = contextAccessor.ConsumeContext is not null;

        EnsureGroupExists(isConsumerContext).Messages.Add(new OutboxMessage
        {
            GroupId = _groupId,
            Destination = context.Destination.ToString(),
            Payload = payload,
            HeadersJson = headersJson,
            Delay = context.Delay,
        });

        if (isConsumerContext)
        {
            // Notify the paired OutboxConsumeMiddleware so it can attempt inline delivery
            // after the consumer's Consume handler (and its SaveChangesAsync) has finished.
            consumeState.GroupId = _groupId;
        }

        // Do not call next — messages are forwarded by OutboxProcessor (or inline delivery in
        // OutboxConsumeMiddleware for consumer-context publishes).
        return Task.CompletedTask;
    }

    private OutboxGroup EnsureGroupExists(bool isConsumerContext)
    {
        if (_outboxGroup is not null)
            return _outboxGroup;

        DateTime? visibleAt = null;
        if (isConsumerContext && options.ConsumerVisibilityTimeout > TimeSpan.Zero)
            visibleAt = DateTime.UtcNow + options.ConsumerVisibilityTimeout;

        _outboxGroup = new OutboxGroup { Id = _groupId, VisibleAt = visibleAt };
        dbContext.OutboxGroups.Add(_outboxGroup);
        return _outboxGroup;
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);
}
