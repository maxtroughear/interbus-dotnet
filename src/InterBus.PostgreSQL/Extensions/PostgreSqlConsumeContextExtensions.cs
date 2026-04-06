using Npgsql;

namespace InterBus.PostgreSQL.Extensions;

/// <summary>
/// Extension methods on <see cref="IConsumeContext"/> for accessing
/// PostgreSQL-specific transport state.
/// </summary>
public static class PostgreSqlConsumeContextExtensions
{
    /// <summary>
    /// Returns the <see cref="NpgsqlTransaction"/> opened by the
    /// <see cref="PostgreSqlMessagePumpMode.Transactional"/> message pump for the current
    /// message dispatch, or <c>null</c> if no ambient transaction is available.
    /// <para>
    /// When a non-<c>null</c> transaction is returned, the consumer may enlist its own
    /// database operations in the same transaction to achieve exactly-once processing:
    /// the message acknowledgment and the application-side database writes are committed
    /// (or rolled back) atomically.
    /// </para>
    /// <para>
    /// <b>Important:</b> all enlisted operations must use <c>tx.Connection</c> — the same
    /// <see cref="NpgsqlConnection"/> the pump opened. A transaction is bound to its
    /// originating connection; assigning it to a command on a different connection will
    /// throw at runtime.
    /// </para>
    /// <para>
    /// The returned transaction must <b>not</b> be committed or rolled back by the caller.
    /// Lifetime management is owned by the message pump.
    /// </para>
    /// <para><b>ADO.NET</b></para>
    /// <code>
    /// var tx = context.GetPostgreSqlTransaction();
    /// await using var cmd = tx!.Connection!.CreateCommand();
    /// cmd.Transaction = tx;
    /// cmd.CommandText = "INSERT INTO processed_orders (id) VALUES ($1)";
    /// cmd.Parameters.Add(new NpgsqlParameter&lt;Guid&gt; { TypedValue = message.OrderId });
    /// await cmd.ExecuteNonQueryAsync(cancellationToken);
    /// </code>
    /// <para><b>Dapper</b></para>
    /// <code>
    /// var tx = context.GetPostgreSqlTransaction();
    /// await tx!.Connection!.ExecuteAsync(
    ///     "INSERT INTO processed_orders (id) VALUES (@Id)",
    ///     new { Id = message.OrderId },
    ///     transaction: tx);
    /// </code>
    /// <para><b>EF Core</b></para>
    /// <code>
    /// var tx = context.GetPostgreSqlTransaction();
    /// dbContext.Database.SetDbConnection(tx!.Connection);
    /// await dbContext.Database.UseTransactionAsync(tx, cancellationToken);
    /// dbContext.ProcessedOrders.Add(new ProcessedOrder { Id = message.OrderId });
    /// await dbContext.SaveChangesAsync(cancellationToken);
    /// </code>
    /// </summary>
    /// <param name="context">The current consume context.</param>
    /// <returns>The shared <see cref="NpgsqlTransaction"/>, or <c>null</c>.</returns>
    public static NpgsqlTransaction? GetPostgreSqlTransaction(this IConsumeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return PostgreSqlTransactionContext.Current;
    }
}
