using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Provides the database-specific strategy for fetching and locking a group of pending outbox
/// messages so that concurrent processor instances do not process the same messages.
/// <para>
/// Implement this interface using the locking primitives of your database provider:
/// <list type="bullet">
///   <item>PostgreSQL — use <see cref="PostgreSqlOutboxLockProvider"/>.</item>
///   <item>SQL Server — use <see cref="SqlServerOutboxLockProvider"/>.</item>
///   <item>Single-instance / testing — use the built-in default registered by
///   <c>UseOutbox&lt;TDbContext&gt;()</c> when no explicit provider is configured.</item>
/// </list>
/// </para>
/// </summary>
public interface IOutboxLockProvider
{
    /// <summary>
    /// Fetches and locks the next group of pending outbox messages for processing.
    /// Must be called within an active database transaction (started by the caller).
    /// <para>
    /// A "group" is all messages that share the same <see cref="OutboxMessage.GroupId"/>,
    /// i.e. all messages published within the same DI scope / unit-of-work.
    /// </para>
    /// </summary>
    /// <param name="context">The DbContext with an active transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// All messages in the locked group, ordered by <see cref="OutboxMessage.CreatedAt"/>.
    /// Returns an empty collection when no pending messages are available.
    /// </returns>
    Task<IReadOnlyList<OutboxMessage>> TryFetchGroupAsync(
        DbContext context,
        CancellationToken cancellationToken);
}
