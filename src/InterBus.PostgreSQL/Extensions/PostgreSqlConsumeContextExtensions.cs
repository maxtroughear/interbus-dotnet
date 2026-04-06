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
    /// The returned transaction must <b>not</b> be committed or rolled back by the caller.
    /// Lifetime management is owned by the message pump.
    /// </para>
    /// </summary>
    /// <param name="context">The current consume context.</param>
    /// <returns>The shared <see cref="NpgsqlTransaction"/>, or <c>null</c>.</returns>
    public static NpgsqlTransaction? GetPostgreSqlTransaction(this IConsumeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return PostgreSqlTransactionContext.Current;
    }
}
