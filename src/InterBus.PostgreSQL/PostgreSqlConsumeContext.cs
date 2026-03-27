using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Scoped service that exposes the active PostgreSQL connection and transaction to consumers.
/// <para>
/// Inject this service in your consumer to participate in the same database transaction that
/// the message pump uses to lock, process, and delete the message. All operations enlisted in
/// <see cref="Transaction"/> are committed atomically with the message deletion on success, or
/// rolled back together on failure, giving transactional processing without an outbox.
/// </para>
/// <para>
/// <strong>Important:</strong> do not commit or roll back <see cref="Transaction"/> yourself;
/// the message pump owns the transaction lifecycle.
/// </para>
/// </summary>
public sealed class PostgreSqlConsumeContext
{
    /// <summary>
    /// The database connection for the current message-processing scope.
    /// </summary>
    public NpgsqlConnection? Connection { get; internal set; }

    /// <summary>
    /// The active database transaction for the current message-processing scope.
    /// </summary>
    public NpgsqlTransaction? Transaction { get; internal set; }
}
