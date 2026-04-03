using System.Collections.Generic;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.EFCore.Internal;

/// <summary>
/// Background service that periodically reads pending messages from the transactional outbox
/// and forwards them to the configured <see cref="IInterBusTransport"/> via
/// <see cref="IInterBusTransportFactory"/>.
/// <para>
/// Sends at most <see cref="OutboxOptions.BatchSize"/> messages per poll, ordered by
/// <see cref="OutboxMessage.CreatedAt"/> ascending (oldest first). Messages with a
/// <see cref="OutboxMessage.VisibleAt"/> in the future are skipped until they become
/// eligible.
/// </para>
/// <para>
/// Delivery is at-least-once: if the process crashes after forwarding but before marking
/// the message as processed, it will be forwarded again on the next poll. Consumers
/// should be idempotent.
/// </para>
/// </summary>
internal sealed class OutboxProcessor<TDbContext>(
    IServiceScopeFactory scopeFactory,
    IInterBusTransportFactory transportFactory,
    OutboxOptions options,
    ILogger<OutboxProcessor<TDbContext>> logger)
    : BackgroundService
    where TDbContext : DbContext, IOutboxDbContext
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "InterBus outbox: unexpected error during batch processing");
            }

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

    private async Task ProcessBatchAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();

        var now = DateTime.UtcNow;
        var messages = await dbContext.OutboxMessages
            .Where(m => m.ProcessedAt == null && (m.VisibleAt == null || m.VisibleAt <= now))
            .OrderBy(m => m.CreatedAt)
            .Take(options.BatchSize)
            .ToListAsync(cancellationToken);

        if (messages.Count == 0)
            return;

        logger.LogDebug("InterBus outbox: processing {Count} pending message(s)", messages.Count);

        var toRemove = new List<OutboxMessage>();

        foreach (var msg in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                var destination = new Uri(msg.Destination);
                var transport = transportFactory.Resolve(destination);
                var headers = DeserializeHeaders(msg.HeadersJson);
                var sendContext = new TransportSendContext(destination, msg.Payload, headers, delay: null);
                await transport.SendAsync(sendContext, cancellationToken);

                if (options.KeepProcessedMessages)
                    msg.ProcessedAt = DateTime.UtcNow;
                else
                    toRemove.Add(msg);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus outbox: failed to forward message {MessageId} to {Destination}; will retry on next poll",
                    msg.Id,
                    msg.Destination);
            }
        }

        if (toRemove.Count > 0)
            dbContext.OutboxMessages.RemoveRange(toRemove);

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    private static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize(json, OutboxJsonContext.Default.DictionaryStringString)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }
}
