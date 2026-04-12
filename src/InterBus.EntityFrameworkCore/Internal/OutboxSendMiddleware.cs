using System.Text.Json;
using InterBus.Middleware;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Internal;

/// <summary>
/// Middleware that routes outgoing messages through the transactional outbox.
/// <para>
/// <b>Bus context</b> (message published via <see cref="InterBus.IMessagePublisher"/>):
/// the message is written to the outbox table and the pipeline terminates here.
/// The <see cref="OutboxProcessor{TDbContext}"/> background service forwards it to the
/// transport once the caller's database transaction has committed.
/// </para>
/// <para>
/// <b>Consumer context</b> (message published from within a consumer's <c>Consume</c> handler):
/// the middleware first attempts to deliver the message to the transport immediately by calling
/// the next step in the pipeline. If delivery succeeds, no outbox entry is created and no
/// background processing is needed. If delivery fails, the message is written to the outbox
/// with a <see cref="OutboxGroup.VisibleAt"/> timestamp equal to
/// <c>UtcNow + <see cref="OutboxOptions.ConsumerVisibilityTimeout"/></c>, which prevents the
/// outbox processor from picking up the group before the consumer's surrounding transaction
/// has had time to commit.
/// </para>
/// <para>
/// All messages that fall back to the outbox within the same DI scope share the same
/// <see cref="OutboxMessage.GroupId"/>, allowing the background processor to fetch and
/// forward them as a single atomic group.
/// </para>
/// </summary>
internal sealed class OutboxSendMiddleware<TDbContext>(
    TDbContext dbContext,
    IMessageSerializer serializer,
    OutboxOptions options,
    IConsumeContextAccessor contextAccessor)
    : IPublishMiddleware
    where TDbContext : DbContext, IOutboxDbContext
{
    /// <summary>Shared by all messages that fall back to the outbox within this DI scope.</summary>
    private readonly Guid _groupId = Guid.CreateVersion7();

    private OutboxGroup? _outboxGroup;

    public async Task InvokeAsync<T>(PublishContext<T> context, Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (contextAccessor.ConsumeContext is not null)
        {
            // Consumer context: attempt immediate delivery to the transport. If delivery succeeds,
            // no outbox entry is needed (avoids a background-processor round trip and any risk of
            // the processor trying to duplicate a message that was already forwarded).
            // If delivery fails, fall back to the outbox so the processor can retry.
            try
            {
                await next(context, cancellationToken);
                return; // Inline delivery succeeded — no outbox entry needed.
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                // Inline delivery failed — write to outbox with a visibility timeout so the
                // processor does not attempt to lock the entry before the surrounding transaction
                // has had time to commit.
                var visibleAt = options.ConsumerVisibilityTimeout > TimeSpan.Zero
                    ? DateTime.UtcNow + options.ConsumerVisibilityTimeout
                    : (DateTime?)null;
                WriteToOutbox(context, visibleAt);
                return;
            }
        }

        // Bus context: write to outbox. The message will be forwarded by OutboxProcessor once
        // the surrounding database transaction has committed.
        WriteToOutbox(context, visibleAt: null);

        // Do not call next — forwarding is handled by OutboxProcessor.
    }

    private void WriteToOutbox<T>(PublishContext<T> context, DateTime? visibleAt) where T : class
    {
        var payload = serializer.Serialize(context.Message);
        var headersJson = SerializeHeaders(context.Headers);

        EnsureGroupExists(visibleAt).Messages.Add(new OutboxMessage
        {
            GroupId = _groupId,
            Destination = context.Destination.ToString(),
            Payload = payload,
            HeadersJson = headersJson,
            Delay = context.Delay,
        });
    }

    private OutboxGroup EnsureGroupExists(DateTime? visibleAt)
    {
        if (_outboxGroup is not null)
            return _outboxGroup;

        _outboxGroup = new OutboxGroup { Id = _groupId, VisibleAt = visibleAt };
        dbContext.OutboxGroups.Add(_outboxGroup);
        return _outboxGroup;
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, OutboxJsonContext.Default.DictionaryStringString);
}
