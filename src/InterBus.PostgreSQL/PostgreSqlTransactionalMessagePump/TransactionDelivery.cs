using InterBus.Dispatching;
using InterBus.MessagePumps;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

internal sealed partial class PostgreSqlTransactionalMessagePump
{
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

        public TransactionDelivery(NpgsqlConnection connection, NpgsqlTransaction transaction, string schema, string queue, long id, long payloadId, byte[] payload, string headers)
        {
            _connection = connection;
            _transaction = transaction;
            _schema = EscapeIdentifier(schema);
            _queue = queue;
            _id = id;
            _payloadId = payloadId;
            Payload = payload;
            try
            {
                Headers = PostgreSqlTransport.DeserializeHeaders(headers);
            }
            catch (Exception error)
            {
                DecodeError = error;
            }
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
                    command.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { TypedValue = result.DeadLetteredAtUtc!.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz });
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(result));
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
