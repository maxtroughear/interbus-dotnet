using InterBus.Consuming;
using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InterBus.PostgreSQL;

internal abstract class PostgreSqlMessagePumpBase(
    NpgsqlDataSource dataSource,
    PostgreSqlTransportOptions options,
    InterBusOptions busOptions,
    IServiceScopeFactory scopeFactory,
    ILogger<PostgreSqlMessagePumpBase> logger)
    : BackgroundService
{
    protected readonly NpgsqlDataSource DataSource = dataSource;
    protected readonly PostgreSqlTransportOptions Options = options;

    // Populated in StartAsync (before base.StartAsync) so topic subscriptions are in the DB
    // before StartAsync returns. BackgroundService guarantees ExecuteAsync runs only after
    // StartAsync completes, so ExecuteAsync can read this field without synchronization.
    private Dictionary<string, long>? _queueIds;

    protected abstract string PumpModeName { get; }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (busOptions.Consumers.Count > 0)
        {
            _queueIds = await PostgreSqlConnectionRetry.ExecuteAsync(
                RegisterSubscriptionsAsync,
                Options,
                logger,
                cancellationToken);
        }

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = busOptions.Consumers;
        if (consumers.Count == 0)
        {
            logger.LogInformation("InterBus PostgreSQL: no consumers registered; message pump idle");
            return;
        }

        var tasks = consumers.Select(d =>
            PostgreSqlConnectionRetry.ExecuteAsync(ct =>
                ListenForQueueAsync(d, _queueIds![d.QueueName], ct), Options, logger, stoppingToken));
        await Task.WhenAll(tasks);
    }

    private async Task<Dictionary<string, long>> RegisterSubscriptionsAsync(CancellationToken cancellationToken)
    {
        var queueIds = new Dictionary<string, long>(StringComparer.Ordinal);

        await using var conn = await DataSource.OpenConnectionAsync(cancellationToken);

        await using var ensureQueueCmd = conn.CreateCommand();
        ensureQueueCmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".ensure_queue($1)";
        var pQueueName = new NpgsqlParameter<string>();
        ensureQueueCmd.Parameters.Add(pQueueName);
        await ensureQueueCmd.PrepareAsync(cancellationToken);

        await using var ensureSubCmd = conn.CreateCommand();
        ensureSubCmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".ensure_subscription($1, $2)";
        var pQueueId = new NpgsqlParameter<long>();
        var pTopicName = new NpgsqlParameter<string>();
        ensureSubCmd.Parameters.Add(pQueueId);
        ensureSubCmd.Parameters.Add(pTopicName);
        await ensureSubCmd.PrepareAsync(cancellationToken);

        foreach (var descriptor in busOptions.Consumers)
        {
            if (!queueIds.ContainsKey(descriptor.QueueName))
            {
                pQueueName.TypedValue = descriptor.QueueName;
                var queueId = (long)(await ensureQueueCmd.ExecuteScalarAsync(cancellationToken))!;
                queueIds[descriptor.QueueName] = queueId;
            }

            var currentQueueId = queueIds[descriptor.QueueName];
            foreach (var messageType in descriptor.MessageTypes)
            {
                var topicName = busOptions.GetPublisherDescriptor(messageType).TopicName;

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
        var concurrency = Math.Max(1, descriptor.ConcurrencyCount);

        logger.LogDebug(
            "InterBus PostgreSQL: starting {PumpMode} pump for queue {QueueName} with concurrency {Concurrency}",
            PumpModeName,
            descriptor.QueueName,
            concurrency);

        await using var listenConn = await DataSource.OpenConnectionAsync(stoppingToken);

        await using (var listenCmd = listenConn.CreateCommand())
        {
            listenCmd.CommandText = $"LISTEN \"{channelName}\"";
            await listenCmd.ExecuteNonQueryAsync(stoppingToken);
        }

        using var concurrencyGate = new SemaphoreSlim(concurrency, concurrency);
        var activeWorkers = new List<Task>(concurrency);

        await StartWorkersAsync(
            descriptor,
            queueId,
            concurrency,
            concurrencyGate,
            activeWorkers,
            stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CleanCompletedWorkersAsync(activeWorkers, stoppingToken);

                using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                pollCts.CancelAfter(Options.FallbackPollInterval);

                var isFallbackPoll = false;
                try
                {
                    await listenConn.WaitAsync(pollCts.Token);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    isFallbackPoll = true;
                }

                await StartWorkersAsync(
                    descriptor,
                    queueId,
                    concurrency,
                    concurrencyGate,
                    activeWorkers,
                    stoppingToken);

                if (isFallbackPoll)
                    await CleanupOrphanedPayloadsAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        await DrainWorkersAsync(activeWorkers, stoppingToken);

        logger.LogDebug(
            "InterBus PostgreSQL: {PumpMode} pump for queue {QueueName} stopped",
            PumpModeName,
            descriptor.QueueName);
    }

    protected async Task ProcessPendingAsync(ConsumerDescriptor descriptor, long queueId,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = await TryProcessNextAsync(descriptor, queueId, stoppingToken);
            if (!processed) break;
        }
    }

    private async Task StartWorkersAsync(
        ConsumerDescriptor descriptor,
        long queueId,
        int concurrency,
        SemaphoreSlim concurrencyGate,
        List<Task> activeWorkers,
        CancellationToken stoppingToken)
    {
        await CleanCompletedWorkersAsync(activeWorkers, stoppingToken);

        while (activeWorkers.Count < concurrency && concurrencyGate.Wait(0))
        {
            activeWorkers.Add(RunWorkerAsync(descriptor, queueId, concurrencyGate, stoppingToken));
        }
    }

    private async Task CleanCompletedWorkersAsync(
        List<Task> activeWorkers,
        CancellationToken stoppingToken)
    {
        for (var i = activeWorkers.Count - 1; i >= 0; i--)
        {
            var worker = activeWorkers[i];
            if (!worker.IsCompleted)
                continue;

            activeWorkers.RemoveAt(i);

            try
            {
                await worker;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
        }
    }

    private async Task DrainWorkersAsync(
        List<Task> activeWorkers,
        CancellationToken stoppingToken)
    {
        try
        {
            await Task.WhenAll(activeWorkers);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task RunWorkerAsync(
        ConsumerDescriptor descriptor,
        long queueId,
        SemaphoreSlim concurrencyGate,
        CancellationToken stoppingToken)
    {
        try
        {
            await ProcessPendingAsync(descriptor, queueId, stoppingToken);
        }
        finally
        {
            concurrencyGate.Release();
        }
    }

    protected abstract Task<bool> TryProcessNextAsync(
        ConsumerDescriptor descriptor,
        long queueId,
        CancellationToken stoppingToken);

    protected async Task CleanupOrphanedPayloadsAsync(CancellationToken cancellationToken)
    {
        await using var cmd = DataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".cleanup_orphaned_payloads()";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    protected async Task<DispatchResult> DispatchAsync(
        ConsumerDescriptor descriptor,
        byte[] payload,
        MessageHeaders headers,
        CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
        var context = new DispatchContext(descriptor, payload, headers);
        await executor.ExecuteAsync(context, ct);
        return context.Result;
    }

    protected static string EscapeIdentifier(string name) =>
        name.Replace("\"", "\"\"");
}
