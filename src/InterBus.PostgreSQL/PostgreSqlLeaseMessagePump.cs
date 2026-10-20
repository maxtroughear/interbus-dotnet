using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

internal sealed class PostgreSqlLeaseMessagePump : PostgreSqlConsumerSource, IBufferedSource
{
    private readonly TimeSpan _leaseDuration;
    private readonly TimeSpan _leaseRenewInterval;
    private readonly string _workerInstanceId = $"{Environment.MachineName}/{Guid.NewGuid():N}";
    private readonly ILogger<PostgreSqlLeaseMessagePump> _logger;

    public PostgreSqlLeaseMessagePump(ConsumerDescriptor descriptor, NpgsqlDataSource dataSource,
        PostgreSqlTransportOptions options, InterBusOptions busOptions, ILogger<PostgreSqlLeaseMessagePump> logger)
        : base(descriptor, dataSource, options, busOptions, logger)
    {
        _logger = logger;
        _leaseDuration = options.LeaseDuration;
        _leaseRenewInterval = options.GetEffectiveLeaseRenewInterval();
    }

    public int PrefetchCount => 0;

    public async Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken)
    {
        while (!intakeToken.IsCancellationRequested)
        {
            using var reservation = await sink.ReserveAsync(intakeToken);
            var token = CreateLeaseToken();
            var claim = await PostgreSqlConnectionRetry.ExecuteAsync(
                ct => TryClaimNextAsync(QueueId, token, ct), Options, _logger, intakeToken);
            if (claim is null) { await WaitForWorkAsync(intakeToken); continue; }
            var delivery = new LeaseDelivery(this, claim, token);
            try { intakeToken.ThrowIfCancellationRequested(); reservation.Handoff(delivery); }
            catch { await delivery.DisposeAsync(); throw; }
        }
    }

    private sealed class LeaseDelivery : IDelivery
    {
        private readonly PostgreSqlLeaseMessagePump _owner;
        private readonly ClaimedMessage _claim;
        private readonly string _token;
        private readonly CancellationTokenSource _renewal = new();
        private readonly Task<bool> _renewalTask;
        public ReadOnlyMemory<byte> Payload => _claim.Payload;
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; }

        public LeaseDelivery(PostgreSqlLeaseMessagePump owner, ClaimedMessage claim, string token)
        {
            _owner = owner; _claim = claim; _token = token;
            _renewalTask = owner.RunLeaseRenewalLoopAsync(claim.MessageId, owner.Descriptor.QueueName, token, _renewal.Token);
            try { Headers = PostgreSqlTransport.DeserializeHeaders(claim.HeadersJson); }
            catch (Exception error) { DecodeError = error; }
        }

        public Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken) => result.Kind switch
        {
            DispatchResultKind.Success => _owner.AckClaimAsync(_claim.MessageId, _claim.PayloadId, _token, cancellationToken),
            DispatchResultKind.DeadLetter => _owner.DeadLetterClaimAsync(_claim.MessageId, _claim.PayloadId, _token,
                _owner.Descriptor.QueueName, result, cancellationToken),
            DispatchResultKind.Release => _owner.ReleaseClaimAsync(_claim.MessageId, _owner.Descriptor.QueueName,
                _token, _owner.Options.FailureRedeliveryDelay, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(result))
        };

        public async ValueTask DisposeAsync()
        {
            await _renewal.CancelAsync();
            try { await _renewalTask; }
            finally { _renewal.Dispose(); }
        }
    }

    private async Task<ClaimedMessage?> TryClaimNextAsync(long queueId, string leaseToken,
        CancellationToken cancellationToken)
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

    private async Task<bool> AckClaimAsync(long messageId, long payloadId, string leaseToken,
        CancellationToken cancellationToken)
    {
        await using var cmd = DataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".ack_claim($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageId });
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = payloadId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        return (bool)(await cmd.ExecuteScalarAsync(cancellationToken))!;
    }

    private async Task<bool> DeadLetterClaimAsync(
        long messageId,
        long payloadId,
        string leaseToken,
        string queueName,
        DispatchResult result,
        CancellationToken cancellationToken)
    {
        await using var cmd = DataSource.CreateCommand();
        cmd.CommandText =
            $"SELECT \"{EscapeIdentifier(Options.SchemaName)}\".dead_letter_claim($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = messageId });
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = payloadId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = leaseToken });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = result.DeadLetterReason });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = result.DeadLetterError });
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>
        {
            TypedValue = result.DeadLetteredAtUtc!.Value,
            NpgsqlDbType = NpgsqlDbType.TimestampTz
        });
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

    private string CreateLeaseToken() => $"{_workerInstanceId}/{Guid.NewGuid():N}";

    private sealed record ClaimedMessage(long MessageId, byte[] Payload, string HeadersJson, long PayloadId);
}
