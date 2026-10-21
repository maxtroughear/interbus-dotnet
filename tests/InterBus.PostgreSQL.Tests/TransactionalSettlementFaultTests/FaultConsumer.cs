using InterBus.PostgreSQL.Extensions;

namespace InterBus.PostgreSQL.Tests;

public partial class TransactionalSettlementFaultTests
{
    public sealed class FaultConsumer(State state) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var transaction = context.GetPostgreSqlTransaction() ?? throw new InvalidOperationException("Missing transaction");
            var attempt = state.Attempts.AddOrUpdate(message.OrderId, 1, (_, count) => count + 1);
            await using var command = transaction.Connection!.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT txid_current()";
            state.TransactionIds.Add((long)(await command.ExecuteScalarAsync(cancellationToken))!);
            command.CommandText = $"INSERT INTO {state.Schema}.ledger VALUES ($1, txid_current())";
            command.Parameters.AddWithValue(message.OrderId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            state.Contexts.Add(context);
            if (state.Fault == "disconnect" && attempt == 1 && message.Total == 1)
            {
                state.Claimed.TrySetResult(transaction.Connection.ProcessID);
                await state.Resume.Task.WaitAsync(cancellationToken);
            }

            if (!ReferenceEquals(transaction, context.GetPostgreSqlTransaction()))
                throw new InvalidOperationException("Ambient transaction changed during dispatch");
        }
    }
}
