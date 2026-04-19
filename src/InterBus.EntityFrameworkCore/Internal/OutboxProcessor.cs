using System.Text.Json;
using InterBus.Send;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.EntityFrameworkCore.Internal;

/// <summary>
/// Background service that continuously reads pending groups of messages from the transactional
/// outbox and forwards them to the configured transport via <see cref="IInterBusTransportFactory"/>.
/// <para>
/// Each iteration fetches one complete group (all messages sharing the same
/// <see cref="OutboxMessage.GroupId"/>) within a database transaction, forwards each message
/// to the transport, deletes the rows, and commits. If any message in the group fails to
/// forward, the transaction is rolled back and the entire group is retried on the next poll.
/// </para>
/// <para>
/// The poll delay (<see cref="OutboxOptions.PollInterval"/>) is only applied when the outbox
/// is empty. While messages are available, the processor loops immediately.
/// </para>
/// <para>
/// Delivery is at-least-once: if the process crashes after forwarding but before committing,
/// the group will be forwarded again on restart. Consumers should be idempotent.
/// </para>
/// </summary>
internal sealed partial class OutboxProcessor<TDbContext>(
    IServiceScopeFactory scopeFactory,
    OutboxOptions options,
    IOutboxLockProvider<TDbContext> lockProvider,
    ILogger<OutboxProcessor<TDbContext>> logger)
    : BackgroundService
    where TDbContext : DbContext
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            bool hadMessages;
            try
            {
                hadMessages = await ProcessGroupAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "InterBus outbox: unexpected error during group processing");
                hadMessages = false;
            }

            // Only wait when the outbox was empty; otherwise loop immediately.
            if (!hadMessages)
            {
                try
                {
                    await Task.Delay(options.PollInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Processes one group of messages. Returns <c>true</c> if a group was found and
    /// forwarded (successfully or not); <c>false</c> if the outbox was empty.
    /// </summary>
    private async Task<bool> ProcessGroupAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var pipeline = scope.ServiceProvider.GetRequiredService<SendPipelineExecutor>();

        // Open a transaction so the lock provider can lock rows within it.
        await using var tx = await dbContext.Database.BeginTransactionAsync(cancellationToken);

        try
        {
            var outboxGroup = await lockProvider.TryFetchGroupAsync(dbContext, cancellationToken);

            if (outboxGroup is null)
            {
                await tx.RollbackAsync(cancellationToken);
                return false;
            }

            using var _ = logger.BeginScope(new Dictionary<string, object> { ["GroupId"] = outboxGroup.Id });

            LogOutboxForwardingGroupCount(outboxGroup.Id, outboxGroup.Messages.Count);

            foreach (var msg in outboxGroup.Messages)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var destination = new Uri(msg.Destination);
                    var headers = DeserializeHeaders(msg.HeadersJson);
                    var sendContext = new SendContext(destination, msg.Payload, headers, msg.Delay);
                    await pipeline.ExecuteAsync(sendContext, cancellationToken: cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        "InterBus outbox: failed to forward message {MessageId} to {Destination}",
                        msg.Id, msg.Destination);
                    await tx.RollbackAsync(cancellationToken);
                    return true;
                }
            }

            dbContext.Set<OutboxGroup>().Remove(outboxGroup);
            await dbContext.SaveChangesAsync(cancellationToken);
            await tx.CommitAsync(cancellationToken);

            return true;
        }
        catch (OperationCanceledException)
        {
            await tx.RollbackAsync(cancellationToken);
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "InterBus outbox: unexpected error during group processing");
            await tx.RollbackAsync(cancellationToken);
            return true; // group existed; caller should not wait before retrying
        }
    }

    private static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize(json, OutboxJsonContext.Default.DictionaryStringString)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }

    [LoggerMessage(LogLevel.Debug, "InterBus outbox: forwarding group {GroupId} ({Count} message(s))")]
    partial void LogOutboxForwardingGroupCount(Guid groupId, int count);
}
