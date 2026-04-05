using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

/// <summary>
/// Lease-based PostgreSQL message pump
/// <para>
/// Claims a message by setting a renewable visibility lease in the database, then releases the
/// processing connection while the consumer runs. Ownership-sensitive operations such as renew,
/// acknowledge, and release are validated against the lease token stored on the message row.
/// </para>
/// </summary>
internal sealed class PostgreSqlLeaseMessagePump : PostgreSqlMessagePumpBase
{
    private readonly TimeSpan _leaseDuration;
    private readonly TimeSpan _leaseRenewInterval;
    private readonly string _workerInstanceId = $"{Environment.MachineName}/{Guid.NewGuid():N}";
    private readonly ILogger<PostgreSqlLeaseMessagePump> _logger;

    /// <summary>
    /// Lease-based PostgreSQL message pump.
    /// <para>
    /// Claims a message by setting a renewable visibility lease in the database, then releases the
    /// processing connection while the consumer runs. Ownership-sensitive operations such as renew,
    /// acknowledge, and release are validated against the lease token stored on the message row.
    /// </para>
    /// </summary>
    public PostgreSqlLeaseMessagePump(NpgsqlDataSource dataSource,
        PostgreSqlTransportOptions options,
        InterBusOptions busOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<PostgreSqlLeaseMessagePump> logger) : base(dataSource, options, busOptions, scopeFactory, logger)
    {
        _logger = logger;
        _leaseDuration = options.LeaseDuration;
        _leaseRenewInterval = options.GetEffectiveLeaseRenewInterval();
    }

    protected override string PumpModeName => "lease";

    protected override async Task<bool> TryProcessNextAsync(ConsumerDescriptor descriptor, long queueId, CancellationToken stoppingToken)
    {
        var leaseToken = CreateLeaseToken();
        var claim = await TryClaimNextAsync(queueId, leaseToken, stoppingToken);
        if (claim is null)
            return false;

        var headers = PostgreSqlTransport.DeserializeHeaders(claim.HeadersJson);
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var renewalTask = RunLeaseRenewalLoopAsync(claim.MessageId, descriptor.QueueName, leaseToken, renewalCts.Token);

        try
        {
            await DispatchAsync(descriptor, claim.Payload, headers, stoppingToken);

            await renewalCts.CancelAsync();
            var leaseStillOwned = await AwaitLeaseRenewalLoopAsync(renewalTask);
            var acked = await AckClaimAsync(claim.MessageId, claim.PayloadId, leaseToken, stoppingToken);
            if (!acked)
            {
                _logger.LogWarning(
                    "InterBus PostgreSQL: lease for message {MessageId} on queue {QueueName} was no longer owned when acknowledging completion",
                    claim.MessageId,
                    descriptor.QueueName);
            }
            else if (!leaseStillOwned)
            {
                _logger.LogDebug(
                    "InterBus PostgreSQL: message {MessageId} on queue {QueueName} completed after at least one unsuccessful lease renewal attempt",
                    claim.MessageId,
                    descriptor.QueueName);
            }

            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            await renewalCts.CancelAsync();
            await AwaitLeaseRenewalLoopAsync(renewalTask);
            await BestEffortReleaseClaimAsync(claim.MessageId, descriptor.QueueName, leaseToken);
            throw;
        }
        catch (Exception ex)
        {
            await renewalCts.CancelAsync();
            await AwaitLeaseRenewalLoopAsync(renewalTask);

            _logger.LogWarning(
                ex,
                "InterBus PostgreSQL: lease consumer {ConsumerType} failed for queue {QueueName}; message will be retried",
                descriptor.ConsumerType.Name,
                descriptor.QueueName);

            var released = await ReleaseClaimAsync(
                claim.MessageId,
                descriptor.QueueName,
                leaseToken,
                Options.FailureRedeliveryDelay,
                CancellationToken.None);

            if (!released)
            {
                _logger.LogWarning(
                    "InterBus PostgreSQL: failed to release lease for message {MessageId} on queue {QueueName} because ownership had already changed",
                    claim.MessageId,
                    descriptor.QueueName);
            }

            return false;
        }
    }

    private async Task<ClaimedMessage?> TryClaimNextAsync(long queueId, string leaseToken, CancellationToken cancellationToken)
    {
        await using var conn = await DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT * FROM \"{EscapeIdentifier(Options.SchemaName)}\".claim_next($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queueId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan>
        {
            TypedValue = _leaseDuration,
            NpgsqlDbType = NpgsqlDbType.Interval
        });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new ClaimedMessage(
            reader.GetInt64(0),
            (byte[])reader.GetValue(1),
            reader.GetString(2),
            reader.GetInt64(3));
    }

    private async Task<bool> AckClaimAsync(long messageId, long payloadId, string leaseToken, CancellationToken cancellationToken)
    {
        await using var cmd = DataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".ack_claim($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageId });
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = payloadId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        return (bool)(await cmd.ExecuteScalarAsync(cancellationToken))!;
    }

    private async Task<bool> RenewClaimAsync(long messageId, string leaseToken, CancellationToken cancellationToken)
    {
        await using var cmd = DataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".renew_claim($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan>
        {
            TypedValue = _leaseDuration,
            NpgsqlDbType = NpgsqlDbType.Interval
        });
        return (bool)(await cmd.ExecuteScalarAsync(cancellationToken))!;
    }

    private async Task<bool> ReleaseClaimAsync(
        long messageId,
        string queueName,
        string leaseToken,
        TimeSpan redeliveryDelay,
        CancellationToken cancellationToken)
    {
        var visibleAt = DateTimeOffset.UtcNow.Add(redeliveryDelay > TimeSpan.Zero ? redeliveryDelay : TimeSpan.Zero);

        await using var cmd = DataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".release_claim($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>
        {
            TypedValue = visibleAt,
            NpgsqlDbType = NpgsqlDbType.TimestampTz
        });
        var released = (bool)(await cmd.ExecuteScalarAsync(cancellationToken))!;

        if (released && visibleAt <= DateTimeOffset.UtcNow)
        {
            _logger.LogDebug(
                "InterBus PostgreSQL: released lease for message {MessageId} on queue {QueueName} for immediate retry",
                messageId,
                queueName);
        }

        return released;
    }

    private async Task BestEffortReleaseClaimAsync(long messageId, string queueName, string leaseToken)
    {
        try
        {
            var released = await ReleaseClaimAsync(messageId, queueName, leaseToken, TimeSpan.Zero, CancellationToken.None);
            if (!released)
            {
                _logger.LogDebug(
                    "InterBus PostgreSQL: lease for message {MessageId} on queue {QueueName} was already lost during shutdown",
                    messageId,
                    queueName);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "InterBus PostgreSQL: failed to release lease for message {MessageId} on queue {QueueName} during shutdown",
                messageId,
                queueName);
        }
    }

    private async Task<bool> RunLeaseRenewalLoopAsync(
        long messageId,
        string queueName,
        string leaseToken,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_leaseRenewInterval, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return true;
            }

            try
            {
                var renewed = await RenewClaimAsync(messageId, leaseToken, cancellationToken);
                if (!renewed)
                {
                    _logger.LogWarning(
                        "InterBus PostgreSQL: lost lease ownership for message {MessageId} on queue {QueueName}",
                        messageId,
                        queueName);
                    return false;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "InterBus PostgreSQL: failed to renew lease for message {MessageId} on queue {QueueName}; will retry on the next renewal interval",
                    messageId,
                    queueName);
            }
        }

        return true;
    }

    private static async Task<bool> AwaitLeaseRenewalLoopAsync(Task<bool> renewalTask)
    {
        try
        {
            return await renewalTask;
        }
        catch (OperationCanceledException)
        {
            return true;
        }
    }

    private string CreateLeaseToken() => $"{_workerInstanceId}/{Guid.NewGuid():N}";

    private sealed record ClaimedMessage(long MessageId, byte[] Payload, string HeadersJson, long PayloadId);
}

