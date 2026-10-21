using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;
using Npgsql;
using InterBus.Dispatching;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

internal sealed class PostgreSqlTransactionalMessagePump(
    ConsumerDescriptor descriptor, NpgsqlDataSource dataSource, PostgreSqlTransportOptions options,
    InterBusOptions busOptions, ILogger<PostgreSqlTransactionalMessagePump> logger)
    : PostgreSqlConsumerSource(descriptor, dataSource, options, busOptions, logger), IManagedSource
{
    public async Task RunAsync(Func<IDelivery, CancellationToken, Task> process, CancellationToken intakeToken,
        CancellationToken processingToken)
    {
        using var workers = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        await Task.WhenAll(Enumerable.Range(0, Descriptor.ConcurrencyCount).Select(_ => WorkerAsync()));

        async Task WorkerAsync()
        {
            try
            {
                await PostgreSqlConnectionRetry.ExecuteAsync(async ct =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        var version = WorkVersion;
                        var processed = await ProcessNextAsync(process, ct, processingToken);
                        if (!processed) await WaitForWorkAsync(version, ct);
                    }
                }, Options, logger, workers.Token);
            }
            catch (OperationCanceledException) when (workers.IsCancellationRequested) { }
            catch (Exception error) { ReportFailure(error); await workers.CancelAsync(); throw; }
        }
    }

    private async Task<bool> ProcessNextAsync(Func<IDelivery, CancellationToken, Task> process,
        CancellationToken intakeToken, CancellationToken processingToken)
    {
        await using var connection = await DataSource.OpenConnectionAsync(intakeToken);
        await using var transaction = await connection.BeginTransactionAsync(intakeToken);
        await using var select = connection.CreateCommand();
        select.Transaction = transaction;
        select.CommandText = $"SELECT * FROM \"{EscapeIdentifier(Options.SchemaName)}\".consume_next($1)";
        select.Parameters.Add(new NpgsqlParameter<long> { TypedValue = QueueId });
        await using var reader = await select.ExecuteReaderAsync(intakeToken);
        if (!await reader.ReadAsync(intakeToken)) return false;
        var id = reader.GetInt64(0);
        var payload = (byte[])reader.GetValue(1);
        var headers = reader.GetString(2);
        var payloadId = reader.GetInt64(3);
        await reader.CloseAsync();

        // Establish ambient state in the same execution flow that awaits core processing
        var previous = PostgreSqlTransactionContext.Current.Value;
        PostgreSqlTransactionContext.Current.Value = transaction;
        try
        {
            var delivery = new TransactionDelivery(connection, transaction, Options.SchemaName,
                Descriptor.QueueName, id, payloadId, payload, headers);
            await process(delivery, processingToken);
            return delivery.Acknowledged;
        }
        finally { PostgreSqlTransactionContext.Current.Value = previous; }
    }

    private sealed class TransactionDelivery : IDelivery
    {
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction _transaction;
        private readonly string _schema, _queue;
        private readonly long _id, _payloadId;
        public ReadOnlyMemory<byte> Payload { get; }
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; }
        public bool Acknowledged { get; private set; }
        public TransactionDelivery(NpgsqlConnection connection, NpgsqlTransaction transaction, string schema,
            string queue, long id, long payloadId, byte[] payload, string headers)
        {
            _connection = connection; _transaction = transaction; _schema = EscapeIdentifier(schema);
            _queue = queue; _id = id; _payloadId = payloadId; Payload = payload;
            try { Headers = PostgreSqlTransport.DeserializeHeaders(headers); }
            catch (Exception error) { DecodeError = error; }
        }

        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            if (result.Kind == DispatchResultKind.Release)
            {
                await _transaction.RollbackAsync(cancellationToken);
                return true;
            }
            await using var command = _connection.CreateCommand();
            command.Transaction = _transaction;
            command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = _id });
            command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = _payloadId });
            switch (result.Kind)
            {
                case DispatchResultKind.Success:
                    command.CommandText = $"SELECT \"{_schema}\".ack_message($1, $2)";
                    break;
                case DispatchResultKind.DeadLetter:
                    command.CommandText = $"SELECT \"{_schema}\".dead_letter_message($1, $2, $3, $4, $5, $6)";
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = _queue });
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = result.DeadLetterReason });
                    command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = result.DeadLetterError });
                    command.Parameters.Add(new NpgsqlParameter<DateTimeOffset>
                    { TypedValue = result.DeadLetteredAtUtc!.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz });
                    break;
                default: throw new ArgumentOutOfRangeException(nameof(result));
            }
            await command.ExecuteNonQueryAsync(cancellationToken);
            await _transaction.CommitAsync(cancellationToken);
            Acknowledged = result.Kind == DispatchResultKind.Success;
            return true;
        }

        // The worker owns connection/transaction disposal, including rollback after ambiguous commit
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
