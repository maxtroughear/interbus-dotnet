using InterBus.PostgreSQL.Extensions;

namespace InterBus.PostgreSQL.Tests;

public partial class TransactionContextTests
{
    public sealed class TransactionConsumer(Transactions state) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var transaction = context.GetPostgreSqlTransaction() ?? throw new InvalidOperationException("Missing ambient transaction");
            await using var command = transaction.Connection!.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT txid_current()";
            var transactionId = (long)(await command.ExecuteScalarAsync(cancellationToken))!;
            command.CommandText = "SELECT 42";
            var value = (int)(await command.ExecuteScalarAsync(cancellationToken))!;
            if (state.UseLedger)
            {
                command.CommandText = "INSERT INTO test_ledger VALUES (42)";
                await command.ExecuteNonQueryAsync(cancellationToken);
                if (Interlocked.Increment(ref state.Attempts) == 1)
                    ((InterBus.Consuming.ConsumeContext<OrderPlaced>)context).Result = InterBus.Dispatching.DispatchResult.Release(new IOException("retry"));
            }

            await Task.Yield();
            if (!ReferenceEquals(transaction, context.GetPostgreSqlTransaction()))
                throw new InvalidOperationException("Transaction changed within a delivery");
            state.Seen.Add((transactionId, value));
        }
    }
}
