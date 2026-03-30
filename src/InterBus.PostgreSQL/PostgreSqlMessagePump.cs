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
/// At startup the pump registers its topic subscriptions via the <c>ensure_queue</c> and
/// <c>ensure_subscription</c> stored functions so that publishers in other processes can
/// discover which queues should receive messages for each topic.
/// </para>
/// <para>
/// Each queue maintains a dedicated long-lived connection for <c>LISTEN</c>. When a
/// <c>NOTIFY</c> is received, the pump drains all pending messages from the database using
/// <c>consume_next</c> (which issues <c>SELECT … FOR UPDATE SKIP LOCKED</c>) inside an
/// explicit transaction. On successful dispatch <c>ack_message</c> deletes the routing row
/// and the payload (when no other queue still references it) in a single round trip. On
/// failure the transaction is rolled back so the message remains available for redelivery.
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
        // Returns the stable numeric queue IDs used as LISTEN/NOTIFY channel names.
        var queueIds = await RegisterSubscriptionsAsync(stoppingToken);

        // Start one listener per consumer queue; all run concurrently.
        var tasks = consumers.Select(d => ListenForQueueAsync(d, queueIds[d.QueueName], stoppingToken));
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Ensures each consumer queue and its topic subscriptions exist in the database using
    /// the <c>ensure_queue</c> and <c>ensure_subscription</c> stored functions. Uses a
    /// SELECT-first strategy inside each function so concurrent pump startups do not conflict.
    /// </summary>
    /// <returns>A map of queue name → numeric queue ID.</returns>
    private async Task<Dictionary<string, long>> RegisterSubscriptionsAsync(CancellationToken cancellationToken)
    {
        var queueIds = new Dictionary<string, long>(StringComparer.Ordinal);

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);

        await using var ensureQueueCmd = conn.CreateCommand();
        ensureQueueCmd.CommandText =
            $"SELECT \"{EscapeIdentifier(_options.SchemaName)}\".ensure_queue($1)";
        var pQueueName = new NpgsqlParameter<string>();
        ensureQueueCmd.Parameters.Add(pQueueName);
        await ensureQueueCmd.PrepareAsync(cancellationToken);

        await using var ensureSubCmd = conn.CreateCommand();
        ensureSubCmd.CommandText =
            $"SELECT \"{EscapeIdentifier(_options.SchemaName)}\".ensure_subscription($1, $2)";
        var pQueueId = new NpgsqlParameter<long>();
        var pTopicName = new NpgsqlParameter<string>();
        ensureSubCmd.Parameters.Add(pQueueId);
        ensureSubCmd.Parameters.Add(pTopicName);
        await ensureSubCmd.PrepareAsync(cancellationToken);

        foreach (var descriptor in _busOptions.Consumers)
        {
            // Ensure the queue exists and get its stable numeric ID (once per unique queue name).
            if (!queueIds.ContainsKey(descriptor.QueueName))
            {
                pQueueName.TypedValue = descriptor.QueueName;
                var queueId = (long)(await ensureQueueCmd.ExecuteScalarAsync(cancellationToken))!;
                queueIds[descriptor.QueueName] = queueId;
            }

            var currentQueueId = queueIds[descriptor.QueueName];
            foreach (var messageType in descriptor.MessageTypes)
            {
                var topicName = _busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                pQueueId.TypedValue = currentQueueId;
                pTopicName.TypedValue = topicName;
                await ensureSubCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        return queueIds;
    }

    private async Task ListenForQueueAsync(ConsumerDescriptor descriptor, long queueId, CancellationToken stoppingToken)
    {
        var channelName = $"ib_{queueId}";

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
        await ProcessPendingAsync(descriptor, queueId, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Wait for a NOTIFY or the fallback poll interval (whichever comes first).
                // The fallback ensures that failed messages are retried even if no new
                // notification arrives.
                using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                pollCts.CancelAfter(_options.FallbackPollInterval);

                var isFallbackPoll = false;
                try
                {
                    await listenConn.WaitAsync(pollCts.Token);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    // Fallback poll interval elapsed — check for pending messages.
                    isFallbackPoll = true;
                }

                await ProcessPendingAsync(descriptor, queueId, stoppingToken);

                // On fallback polls, also clean up any orphaned payload rows that may have
                // been left behind by concurrent fan-out processing.
                if (isFallbackPoll)
                    await CleanupOrphanedPayloadsAsync(stoppingToken);
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
    private async Task ProcessPendingAsync(ConsumerDescriptor descriptor, long queueId, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = await TryProcessNextAsync(descriptor, queueId, stoppingToken);
            if (!processed) break;
        }
    }

    /// <summary>
    /// Attempts to fetch and dispatch the next pending message for <paramref name="descriptor"/>.
    /// Uses <c>consume_next</c> (SELECT FOR UPDATE SKIP LOCKED) inside an explicit transaction
    /// so the row lock persists until the consumer succeeds or the transaction is rolled back.
    /// On success, <c>ack_message</c> deletes the routing row and conditionally the payload in
    /// a single round trip.
    /// </summary>
    /// <returns><c>true</c> if a message was dispatched; <c>false</c> if the queue was empty.</returns>
    private async Task<bool> TryProcessNextAsync(ConsumerDescriptor descriptor, long queueId, CancellationToken stoppingToken)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(stoppingToken);
        await using var tx = await conn.BeginTransactionAsync(stoppingToken);

        // Lock one pending message routing row without locking the shared payload row so
        // concurrent pumps on different queues don't block each other.
        await using var selectCmd = conn.CreateCommand();
        selectCmd.Transaction = tx;
        selectCmd.CommandText =
            $"SELECT * FROM \"{EscapeIdentifier(_options.SchemaName)}\".consume_next($1)";
        selectCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queueId });

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
        var payloadId = reader.GetInt64(3);
        await reader.CloseAsync();

        var headers = PostgreSqlTransport.DeserializeHeaders(headersJson);

        try
        {
            await DispatchAsync(descriptor, payload, headers, stoppingToken);

            // Delete the routing row and (when unreferenced) the shared payload in one round trip.
            await using var ackCmd = conn.CreateCommand();
            ackCmd.Transaction = tx;
            ackCmd.CommandText =
                $"SELECT \"{EscapeIdentifier(_options.SchemaName)}\".ack_message($1, $2)";
            ackCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
            ackCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = payloadId });
            await ackCmd.ExecuteNonQueryAsync(stoppingToken);

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

    /// <summary>
    /// Deletes any <c>message_payloads</c> rows that are no longer referenced by any routing
    /// row. This handles the rare race where two concurrent pump instances both skipped the
    /// inline cleanup in <c>ack_message</c> because they each saw the other's routing row as
    /// still committed at the time of the <c>NOT EXISTS</c> check.
    /// </summary>
    private async Task CleanupOrphanedPayloadsAsync(CancellationToken cancellationToken)
    {
        await using var cmd = _dataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(_options.SchemaName)}\".cleanup_orphaned_payloads()";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task DispatchAsync(
        ConsumerDescriptor descriptor,
        byte[] payload,
        MessageHeaders headers,
        CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
        await executor.ExecuteAsync(descriptor, payload, headers, ct);
    }

    private static string EscapeIdentifier(string name) =>
        name.Replace("\"", "\"\"");
}
