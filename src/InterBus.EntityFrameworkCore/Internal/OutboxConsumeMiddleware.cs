using System.Text.Json;
using InterBus.Consumer;
using InterBus.Send;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace InterBus.EntityFrameworkCore.Internal;

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
/// <c>IConsumerBuilder.UseOutboxWithInlineDelivery&lt;TDbContext&gt;()</c>) because it relies on
/// <see cref="OutboxConsumeState"/> being populated by the publish middleware.
/// </para>
/// </summary>
internal sealed partial class OutboxConsumeMiddleware<TDbContext>(
    TDbContext dbContext,
    OutboxConsumeState consumeState,
    SendPipelineExecutor pipeline,
    ILogger<OutboxConsumeMiddleware<TDbContext>> logger)
    : IConsumeMiddleware
    where TDbContext : DbContext
{
    public async Task InvokeAsync<T>(ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        // Run the consumer handler. The consumer is expected to call SaveChangesAsync inside
        // Consume, which commits any outbox entries written by OutboxPublishMiddleware to the DB.
        await next(context, cancellationToken);

        if (consumeState.GroupId is not { } groupId)
            return;

        var outboxGroup = await dbContext.Set<OutboxGroup>()
            .AsNoTracking()
            .Include(g => g.Messages)
            .FirstOrDefaultAsync(g => g.Id == groupId, cancellationToken);

        if (outboxGroup is null)
            return;

        LogInlineDelivery(outboxGroup.Id, outboxGroup.Messages.Count);

        // Attempt inline delivery for each message. If any message fails, swallow the exception
        // and leave the group in the DB for the background processor to retry.
        try
        {
            foreach (var msg in outboxGroup.Messages.OrderBy(m => m.Id))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var destination = new Uri(msg.Destination);
                var headers = DeserializeHeaders(msg.HeadersJson);
                var sendContext = new SendContext(destination, msg.Payload, headers, msg.Delay);
                await pipeline.ExecuteAsync(sendContext, cancellationToken: cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Inline delivery failed - leave the group for the background processor.
            LogInlineDeliveryFailed(outboxGroup.Id, ex);
            return;
        }

        try
        {
            await dbContext.Set<OutboxGroup>()
                .Where(g => g.Id == groupId)
                .ExecuteDeleteAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            // Delete failed. The processor will pick up and re-deliver the group. Consumers
            // should be idempotent.
            LogInlineDeleteFailed(outboxGroup.Id, ex);
        }
    }

    private static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize(json, OutboxJsonContext.Default.DictionaryStringString)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }

    [LoggerMessage(LogLevel.Debug,
        "InterBus outbox: inline delivery for group {GroupId} ({Count} message(s))")]
    partial void LogInlineDelivery(Guid groupId, int count);

    [LoggerMessage(LogLevel.Warning,
        "InterBus outbox: inline delivery failed for group {GroupId}; group left for background processor")]
    partial void LogInlineDeliveryFailed(Guid groupId, Exception ex);

    [LoggerMessage(LogLevel.Warning,
        "InterBus outbox: failed to delete inline-delivered group {GroupId}; processor will re-deliver")]
    partial void LogInlineDeleteFailed(Guid groupId, Exception ex);
}
