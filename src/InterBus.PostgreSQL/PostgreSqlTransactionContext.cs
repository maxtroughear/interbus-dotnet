using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Holds the ambient <see cref="NpgsqlTransaction"/> opened by the
/// <see cref="PostgreSqlTransactionalMessagePump"/> for the duration of a single message dispatch.
/// The value flows naturally through the async call chain via <see cref="AsyncLocal{T}"/>,
/// making it accessible from any code that runs within the same asynchronous context,
/// including consumers.
/// </summary>
internal static class PostgreSqlTransactionContext
{
    private static readonly AsyncLocal<NpgsqlTransaction?> _current = new();

    /// <summary>
    /// The ambient transaction for the current message dispatch, or <c>null</c> if the
    /// message pump is not in transactional mode or no dispatch is currently active.
    /// </summary>
    internal static NpgsqlTransaction? Current
    {
        get => _current.Value;
        set => _current.Value = value;
    }
}
