using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that starts one reader
/// task per registered consumer queue and delivers messages to consumers using PostgreSQL's
/// <c>LISTEN</c>/<c>NOTIFY</c> mechanism for push-based, low-latency consumption.
/// <para>
/// At startup the pump registers its topic subscriptions in the <c>subscriptions</c> table so
/// that publishers — which may be running in a separate service — can discover which queues
/// should receive messages for each topic without needing compile-time knowledge of consumers.
/// </para>
/// <para>
/// Each queue maintains a dedicated long-lived connection for <c>LISTEN</c>. When a
/// <c>NOTIFY</c> is received, the pump drains all pending messages from the database using
/// <c>SELECT … FOR UPDATE SKIP LOCKED</c> inside a transaction. On successful dispatch the row
/// is deleted; on failure the transaction is rolled back so the message remains available for
/// redelivery on the next notification or fallback poll.
/// </para>
/// </summary>
internal sealed class PostgreSqlMessagePump : BackgroundService
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlTransportOptions _options;
    private readonly InterBusOptions _busOptions;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<PostgreSqlMessagePump> _logger;

    public PostgreSqlMessagePump(
        NpgsqlDataSource dataSource,
        PostgreSqlTransportOptions options,
        InterBusOptions busOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<PostgreSqlMessagePump> logger)
    {
        _dataSource = dataSource;
        _options = options;
        _busOptions = busOptions;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = _busOptions.Consumers;
        if (consumers.Count == 0)
        {
            _logger.LogInformation("InterBus PostgreSQL: no consumers registered; message pump idle");
            return;
        }

        // Register this service's topic subscriptions in the DB so that publishers in other
        // processes can route messages to this service's queues without knowing about them.
        await RegisterSubscriptionsAsync(stoppingToken);

        // Start one listener per consumer queue; all run concurrently.
        var tasks = consumers.Select(d => ListenForQueueAsync(d, stoppingToken));
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Upserts (queue_name, topic_name) rows into the <c>subscriptions</c> table for every
    /// consumer registered in this service. Safe to call on every startup (idempotent).
    /// </summary>
    private async Task RegisterSubscriptionsAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in _busOptions.Consumers)
        {
            foreach (var messageType in descriptor.MessageTypes)
            {
                var topicName = _busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText =
                    $"INSERT INTO \"{EscapeIdentifier(_options.SchemaName)}\".subscriptions " +
                    "(queue_name, topic_name) VALUES ($1, $2) " +
                    "ON CONFLICT (queue_name, topic_name) DO NOTHING";
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = descriptor.QueueName });
                cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = topicName });
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }

    private async Task ListenForQueueAsync(ConsumerDescriptor descriptor, CancellationToken stoppingToken)
    {
        var channelName = PostgreSqlTransport.ChannelName(descriptor.QueueName);

        _logger.LogDebug(
            "InterBus PostgreSQL: starting pump for queue {QueueName}",
            descriptor.QueueName);

        await using var listenConn = await _dataSource.OpenConnectionAsync(stoppingToken);

        // Start LISTEN before draining pending rows so no notifications are missed.
        await using (var listenCmd = listenConn.CreateCommand())
        {
            listenCmd.CommandText = $"LISTEN \"{channelName}\"";
            await listenCmd.ExecuteNonQueryAsync(stoppingToken);
        }

        // Drain any messages that arrived before we started listening (e.g. after a restart).
        await ProcessPendingAsync(descriptor, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Wait for a NOTIFY or the fallback poll interval (whichever comes first).
                // The fallback ensures that failed messages are retried even if no new
                // notification arrives.
                using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                pollCts.CancelAfter(_options.FallbackPollInterval);

                try
                {
                    await listenConn.WaitAsync(pollCts.Token);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    // Fallback poll interval elapsed — check for pending messages.
                }

                await ProcessPendingAsync(descriptor, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogDebug(
            "InterBus PostgreSQL: pump for queue {QueueName} stopped",
            descriptor.QueueName);
    }

    /// <summary>
    /// Processes all pending messages for <paramref name="descriptor"/> in a tight loop until
    /// no more unprocessed rows remain.
    /// </summary>
    private async Task ProcessPendingAsync(ConsumerDescriptor descriptor, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = await TryProcessNextAsync(descriptor, stoppingToken);
            if (!processed) break;
        }
    }

    /// <summary>
    /// Attempts to fetch and dispatch the next pending message for <paramref name="descriptor"/>.
    /// </summary>
    /// <returns><c>true</c> if a message was dispatched; <c>false</c> if the queue was empty.</returns>
    private async Task<bool> TryProcessNextAsync(ConsumerDescriptor descriptor, CancellationToken stoppingToken)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(stoppingToken);
        await using var tx = await conn.BeginTransactionAsync(stoppingToken);

        // Lock one pending message; SKIP LOCKED ensures competing pump instances don't clash.
        await using var selectCmd = conn.CreateCommand();
        selectCmd.Transaction = tx;
        selectCmd.CommandText =
            $"SELECT id, payload, headers FROM \"{EscapeIdentifier(_options.SchemaName)}\".messages " +
            "WHERE queue_name = $1 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1";
        selectCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = descriptor.QueueName });

        await using var reader = await selectCmd.ExecuteReaderAsync(stoppingToken);
        if (!await reader.ReadAsync(stoppingToken))
        {
            await reader.CloseAsync();
            await tx.RollbackAsync(CancellationToken.None);
            return false;
        }

        var id = reader.GetInt64(0);
        var payload = (byte[])reader.GetValue(1);
        var headersJson = reader.GetString(2);
        await reader.CloseAsync();

        var headers = PostgreSqlTransport.DeserializeHeaders(headersJson);

        try
        {
            await DispatchAsync(descriptor, payload, headers, conn, tx, stoppingToken);

            // Delete the row on successful dispatch.
            await using var deleteCmd = conn.CreateCommand();
            deleteCmd.Transaction = tx;
            deleteCmd.CommandText =
                $"DELETE FROM \"{EscapeIdentifier(_options.SchemaName)}\".messages WHERE id = $1";
            deleteCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
            await deleteCmd.ExecuteNonQueryAsync(stoppingToken);

            await tx.CommitAsync(stoppingToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            await tx.RollbackAsync(CancellationToken.None);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "InterBus PostgreSQL: consumer {ConsumerType} failed for queue {QueueName}; message will be retried",
                descriptor.ConsumerType.Name,
                descriptor.QueueName);

            // Roll back so the message remains in the table for the next delivery attempt.
            await tx.RollbackAsync(CancellationToken.None);

            // Return false so the processing loop backs off; the message will be re-delivered
            // on the next NOTIFY or on the next fallback poll.
            return false;
        }
    }

    private async Task DispatchAsync(
        ConsumerDescriptor descriptor,
        byte[] payload,
        MessageHeaders headers,
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();

        // Always populate the scoped PostgreSqlConsumeContext so consumers can optionally
        // inject it to enlist their own operations in the same transaction.
        var consumeCtx = scope.ServiceProvider.GetRequiredService<PostgreSqlConsumeContext>();
        consumeCtx.Connection = conn;
        consumeCtx.Transaction = tx;

        var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
        await executor.ExecuteAsync(descriptor, payload, headers, ct);
    }

    private static string EscapeIdentifier(string name) =>
        name.Replace("\"", "\"\"");
}
