using System.Text.Json;
using InterBus.Sending;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Background service that continuously reads pending groups of messages from the transactional
/// outbox and forwards them to the configured transport via <see cref="ITransportFactory"/>.
/// <para>
/// On startup, ensures one <see cref="OutboxLock"/> row exists per configured shard. Each
/// processing iteration claims the oldest available shard, fetches the oldest pending
/// <see cref="OutboxGroup"/> within it, and forwards every message in that group to the
/// transport — guaranteeing FIFO delivery within each shard.
/// </para>
/// <para>
/// The empty poll delay (<see cref="OutboxOptions.EmptyPollDelay"/>) is only applied when no
/// claimable groups are available. While messages are available, workers loop immediately.
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
        await InitializeShardLocksAsync(stoppingToken);

        var workerCount = options.MaxConcurrentWorkers;
        if (workerCount < 1)
        {
            logger.LogWarning("InterBus outbox: MaxConcurrentWorkers must be >= 1; defaulting to 1");
            workerCount = 1;
        }

        if (lockProvider is EfCoreOutboxLockProvider<TDbContext> && workerCount > 1)
        {
            logger.LogWarning(
                "InterBus outbox: MaxConcurrentWorkers={WorkerCount} requested with default EF lock provider; forcing 1 worker because this provider is single-instance only",
                workerCount);
            workerCount = 1;
        }

        var workers = Enumerable.Range(0, workerCount)
            .Select(workerId => RunWorkerLoopAsync(workerId, stoppingToken));
        await Task.WhenAll(workers);
    }

    private async Task RunWorkerLoopAsync(int workerId, CancellationToken stoppingToken)
    {
        using var _ = logger.BeginScope(new Dictionary<string, object> { ["WorkerId"] = workerId });
        var shardCount = Math.Max(1, options.ShardCount);
        var preferredShard = workerId % shardCount;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                preferredShard = await WorkerLoopAsync(preferredShard, shardCount, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task<int> WorkerLoopAsync(int preferredShard, int shardCount, CancellationToken stoppingToken)
    {
        (bool HadMessages, int? ClaimedShard) result;
        try
        {
            result = await ProcessGroupAsync(preferredShard, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "InterBus outbox: unexpected error during group processing");
            result = (false, null);
        }

        if (result.ClaimedShard is { } claimedShard)
            preferredShard = (claimedShard + 1) % shardCount;

        // Only wait when the outbox was empty; otherwise loop immediately.
        if (!result.HadMessages)
        {
            try
            {
                await Task.Delay(options.EmptyPollDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
        }

        return preferredShard;
    }

    /// <summary>
    /// Initialises <see cref="OutboxLock"/> rows (one per shard) so that DB-level lock
    /// providers can claim shards via row-level locking.
    /// </summary>
    private async Task InitializeShardLocksAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
            await lockProvider.EnsureShardLocksAsync(dbContext, options.ShardCount, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down before the loop even starts — that's fine.
        }
        catch (Exception ex)
        {
            // Non-fatal: processing continues without shard rows; the default EfCore provider
            // does not require them, but DB-specific providers (PostgreSQL, SQL Server) rely on
            // their existence for multi-instance FIFO locking. Missing rows may allow duplicate
            // processing across concurrent instances.
            logger.LogWarning(ex,
                "InterBus outbox: failed to initialize shard lock rows; concurrent instances may process the same shard simultaneously");
        }
    }

    /// <summary>
    /// Processes one group of messages. Returns <c>true</c> if a group was found and
    /// forwarded (successfully or not); <c>false</c> if the outbox was empty.
    /// </summary>
    private async Task<(bool HadMessages, int? ClaimedShard)> ProcessGroupAsync(
        int preferredStartShard,
        CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var pipeline = scope.ServiceProvider.GetRequiredService<SendPipelineExecutor>();

        // Open a transaction so the lock provider can lock rows within it.
        await using var tx = await dbContext.Database.BeginTransactionAsync(cancellationToken);

        try
        {
            var claimed = await lockProvider.TryClaimAndFetchGroupAsync(
                dbContext,
                cancellationToken,
                preferredStartShard);

            if (claimed is null)
            {
                await tx.RollbackAsync(cancellationToken);
                return (false, null);
            }

            var (shard, outboxGroup) = claimed.Value;

            if (outboxGroup is null)
            {
                await tx.RollbackAsync(cancellationToken);
                return (true, shard);
            }

            using var _ = logger.BeginScope(new Dictionary<string, object>
            {
                ["GroupId"] = outboxGroup.Id,
                ["Shard"] = shard,
            });

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
                    return (true, shard);
                }
            }

            dbContext.Set<OutboxGroup>().Remove(outboxGroup);
            await dbContext.SaveChangesAsync(cancellationToken);
            await tx.CommitAsync(cancellationToken);

            return (true, shard);
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
            return (true, null); // group likely existed; caller should not wait before retrying
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
