using System.Text.Json;
using InterBus.Consuming;
using InterBus.Sending;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Consume middleware that attempts immediate ("inline") delivery of outbox messages written by
/// <see cref="OutboxPublishMiddleware{TDbContext}"/> during the same consumer dispatch.
/// <para>
/// After the consumer's <c>Consume</c> handler returns (including any <c>SaveChangesAsync</c>
/// call that commits the outbox group to the database), this middleware reads the group from the
/// database and forwards each message to the transport using
/// <see cref="SendPipelineExecutor"/>, exactly as the background
/// <see cref="OutboxProcessor{TDbContext}"/> would. If every message in the group is delivered
/// successfully the group is deleted from the database in the same scope, saving a background
/// polling round-trip.
/// </para>
/// <para>
/// If delivery fails for any reason the exception is swallowed and the group remains in the
/// database with its <see cref="OutboxGroup.VisibleAt"/> timestamp intact, so the background
/// processor will retry it after the visibility window has elapsed.
/// </para>
/// <para>
/// This middleware must be registered <b>together</b> with
/// <see cref="OutboxPublishMiddleware{TDbContext}"/> on the same consumer (via
/// <c>ConsumerBuilder.UseOutboxWithInlineDelivery&lt;TDbContext&gt;()</c>) because it relies on
/// <see cref="OutboxConsumeState"/> being populated by the publish middleware.
/// </para>
/// </summary>
internal sealed partial class InboxOutboxConsumeMiddleware<TDbContext>(
    TDbContext dbContext,
    OutboxConsumeState consumeState,
    InboxLoopConsumeState inboxLoop,
    SendPipelineExecutor pipeline,
    ILockProvider lockProvider,
    ILogger<InboxOutboxConsumeMiddleware<TDbContext>> logger)
    : IConsumeMiddleware
    where TDbContext : DbContext
{
    public async Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!context.Headers.TryGetValue(InterBusHeaders.MessageId, out var messageId) ||
            string.IsNullOrWhiteSpace(messageId))
        {
            logger.LogWarning(
                "InterBus outbox: message received without {MessageId} header; skipping inbox and outbox delivery",
                InterBusHeaders.MessageId);

            await next(context, cancellationToken);
            return;
        }

        inboxLoop.Loop = false;

        var lockKey = $"{context.Descriptor.ConsumerType.FullName}:{messageId}";
        await using var inboxLock = await lockProvider.AcquireLockAsync(lockKey, cancellationToken);

        var consumerName = context.Descriptor.ConsumerType.FullName ?? context.Descriptor.ConsumerType.Name;
        var inbox = await dbContext.Set<InboxState>()
            .Include(i => i.Messages)
            .FirstOrDefaultAsync(i => i.ConsumerName == consumerName && i.MessageId == messageId, cancellationToken);

        if (inbox is null)
        {
            inbox = new InboxState(consumerName, messageId);
            dbContext.Set<InboxState>().Add(inbox);
        }
        
        consumeState.Inbox = inbox;

        var inboxTask = inbox.State switch
        {
            InboxProcessingState.Completed => Task.CompletedTask,
            InboxProcessingState.PendingConsumer => RunConsumer(inbox, context, next, cancellationToken),
            InboxProcessingState.PendingOutbox => DeliverOutbox(inbox, cancellationToken),
            _ => throw new InvalidOperationException(
                $"Invalid inbox state {inbox.State} for message {messageId} in consumer {consumerName}")
        };

        await inboxTask;
    }

    private async Task RunConsumer<T>(InboxState inbox, ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next, CancellationToken cancellationToken)
    {
        await next(context, cancellationToken);

        if (inbox.Messages.Count == 0)
        {
            inbox.Complete();
            await dbContext.SaveChangesAsync(cancellationToken);
            return;
        }

        inbox.Consume();
        await dbContext.SaveChangesAsync(cancellationToken);
        inboxLoop.Loop = true;
    }

    private async Task DeliverOutbox(InboxState inbox, CancellationToken cancellationToken)
    {
        if (inbox.Messages.Count == 0)
        {
            inbox.Complete();
            await dbContext.SaveChangesAsync(cancellationToken);
            return;
        }

        LogInlineDelivery(inbox.Id, inbox.Messages.Count);

        try
        {
            // TODO: Add limit to send in each loop
            foreach (var msg in inbox.Messages.OrderBy(m => m.Id))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var destination = new Uri(msg.Destination);
                var headers = DeserializeHeaders(msg.HeadersJson);
                var sendContext = new SendContext(destination, msg.Payload, headers, msg.Delay);
                await pipeline.ExecuteAsync(sendContext, cancellationToken: cancellationToken);

                dbContext.Remove(msg);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "InterBus outbox: delivery failed for inbox {InboxId}", inbox.Id);
            throw;
        }

        inbox.Complete();
        await dbContext.SaveChangesAsync(cancellationToken);
    }

    private static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize(json, OutboxJsonContext.Default.DictionaryStringString)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }

    [LoggerMessage(LogLevel.Debug,
        "InterBus outbox: delivery for inbox {InboxId} ({Count} message(s))")]
    partial void LogInlineDelivery(Guid inboxId, int count);
}
