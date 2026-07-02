using System.Collections.Concurrent;
using System.Threading.Channels;
using InterBus.Requests;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

/// <summary>
/// Singleton that manages a single PostgreSQL "host" queue shared by all request/reply
/// exchanges in the current application instance.
/// <para>
/// Each call to <see cref="RegisterAsync"/> allocates a <em>virtual</em> queue — an
/// in-memory <see cref="Channel{T}"/> identified by a unique ID embedded as the
/// <c>interbus-postgresql-vq-id</c> header in the reply message. A single background loop
/// uses PostgreSQL <c>LISTEN</c>/<c>NOTIFY</c> to receive replies efficiently, then routes
/// each message to the correct channel without creating a real queue per exchange.
/// </para>
/// </summary>
internal sealed class PostgreSqlVirtualQueueHost(
    NpgsqlDataSource dataSource,
    PostgreSqlTransportOptions options,
    PostgreSqlTopologyBuilder topologyBuilder,
    ILogger<PostgreSqlVirtualQueueHost> logger)
    : IVirtualQueueHost, IAsyncDisposable
{
    /// <summary>Header key used to route a reply to the correct virtual queue.</summary>
    internal const string VirtualQueueIdHeader = "interbus-postgresql-vq-id";

    private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);

    private readonly SemaphoreSlim _initLock = new(1, 1);

    private readonly ConcurrentDictionary<string, Channel<(byte[] Payload, MessageHeaders Headers)>> _channels =
        new(StringComparer.Ordinal);

    private string? _queueName;
    private long _queueId;
    private CancellationTokenSource? _cts;
    private Task? _listenTask;

    /// <summary>
    /// Lazily creates the shared host queue (once) then allocates a new virtual queue
    /// and returns the host queue name, the virtual queue ID, and a reader end of the
    /// channel that will receive the reply.
    /// </summary>
    public async Task<VirtualQueueRegistration> RegisterAsync(CancellationToken cancellationToken = default)
    {
        await EnsureHostQueueAsync(cancellationToken);

        var vqId = Guid.CreateVersion7().ToString("N");
        var channel = Channel.CreateUnbounded<(byte[], MessageHeaders)>(
            new UnboundedChannelOptions { SingleReader = true });
        _channels[vqId] = channel;

        return new VirtualQueueRegistration(_queueName!, vqId, channel.Reader);
    }

    /// <summary>Completes and removes the channel for <paramref name="vqId"/>.</summary>
    public void Unregister(string vqId)
    {
        if (_channels.TryRemove(vqId, out var channel))
            channel.Writer.TryComplete();
    }

    private async Task EnsureHostQueueAsync(CancellationToken cancellationToken)
    {
        if (_queueName is not null)
            return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_queueName is not null)
                return;

            var rawName = $"interbus.reply.{Guid.CreateVersion7():N}";

            _queueId = await topologyBuilder.EnsureQueueIdAsync(rawName, cancellationToken);
            _queueName = rawName;
            _cts = new CancellationTokenSource();
            _listenTask = Task.Run(() => PollLoopAsync(_cts.Token), CancellationToken.None);

            logger.LogDebug("InterBus PostgreSQL: created virtual queue host {QueueName}", rawName);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task PollLoopAsync(CancellationToken ct)
    {
        var delay = false;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (delay)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    delay = false;
                }

                await using var conn = await dataSource.OpenConnectionAsync(ct);

                await using (var listenCmd = conn.CreateCommand())
                {
                    listenCmd.CommandText = $"LISTEN \"ib_{_queueId}\"";
                    await listenCmd.ExecuteNonQueryAsync(ct);
                }

                await DrainPendingAsync(ct);

                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        using var pollCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        pollCts.CancelAfter(options.FallbackPollInterval);

                        try
                        {
                            await conn.WaitAsync(pollCts.Token);
                        }
                        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                        {
                            // fallback poll timeout — continue to drain
                        }

                        await DrainPendingAsync(ct);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        return;
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus PostgreSQL: error in virtual queue host {QueueName}; retrying",
                    _queueName);

                delay = true;
            }
        }
    }

    private async Task DrainPendingAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var processed = await TryProcessNextAsync(ct);
            if (!processed) break;
        }
    }

    private async Task<bool> TryProcessNextAsync(CancellationToken ct)
    {
        var leaseToken = Guid.NewGuid().ToString("N");

        await using var claimCmd = dataSource.CreateCommand();
        claimCmd.CommandText =
            $"SELECT * FROM \"{EscapeIdentifier(options.SchemaName)}\".claim_next($1, $2, $3)";
        claimCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = _queueId });
        claimCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        claimCmd.Parameters.Add(new NpgsqlParameter<TimeSpan>
        {
            TypedValue = LeaseDuration,
            NpgsqlDbType = NpgsqlDbType.Interval,
        });

        long messageId;
        byte[] payload;
        string headersJson;
        long payloadId;

        await using (var reader = await claimCmd.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct))
                return false;

            messageId = reader.GetInt64(0);
            payload = (byte[])reader.GetValue(1);
            headersJson = reader.GetString(2);
            payloadId = reader.GetInt64(3);
        }

        var headers = PostgreSqlTransport.DeserializeHeaders(headersJson);

        if (!headers.TryGetValue(VirtualQueueIdHeader, out var vqId) ||
            !_channels.TryGetValue(vqId, out var channel))
        {
            logger.LogWarning(
                "InterBus PostgreSQL: received reply on virtual queue host with unknown or missing vq-id; discarding");
        }
        else
        {
            channel.Writer.TryWrite((payload, headers));
        }

        await AckAsync(messageId, payloadId, leaseToken);
        return true;
    }

    private async Task AckAsync(long messageId, long payloadId, string leaseToken)
    {
        await using var cmd = dataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(options.SchemaName)}\".ack_claim($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageId });
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = payloadId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        await cmd.ExecuteScalarAsync(CancellationToken.None);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_cts is not null)
        {
            await _cts.CancelAsync();
            if (_listenTask is not null)
            {
                try
                {
                    await _listenTask;
                }
                catch (Exception ex) when (ex is OperationCanceledException or AggregateException)
                {
                    // expected on cancellation
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "InterBus PostgreSQL: virtual queue host poll loop faulted during shutdown");
                }
            }

            _cts.Dispose();
        }

        foreach (var channel in _channels.Values)
            channel.Writer.TryComplete();
        _channels.Clear();

        if (_queueName is not null)
        {
            try
            {
                var schema = EscapeIdentifier(options.SchemaName);
                await using var messagesCmd = dataSource.CreateCommand();
                messagesCmd.CommandText =
                    $"DELETE FROM \"{schema}\".messages WHERE queue_id = $1";
                messagesCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = _queueId });
                await messagesCmd.ExecuteNonQueryAsync(CancellationToken.None);

                await using var cleanupCmd = dataSource.CreateCommand();
                cleanupCmd.CommandText =
                    $"SELECT \"{schema}\".cleanup_orphaned_payloads()";
                await cleanupCmd.ExecuteNonQueryAsync(CancellationToken.None);

                await using var queueCmd = dataSource.CreateCommand();
                queueCmd.CommandText =
                    $"DELETE FROM \"{schema}\".queues WHERE id = $1";
                queueCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = _queueId });
                await queueCmd.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus PostgreSQL: failed to clean up virtual queue host {QueueName}",
                    _queueName);
            }
        }

        _initLock.Dispose();
    }

    private static string EscapeIdentifier(string name) =>
        name.Replace("\"", "\"\"");
}
