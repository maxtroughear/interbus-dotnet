using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Provides the database-specific strategy for fetching and locking a group of pending outbox
/// messages so that concurrent processor instances do not process the same messages.
/// <para>
/// Implement this interface using the locking primitives of your database provider:
/// <list type="bullet">
///   <item>PostgreSQL — use <see cref="PostgreSqlOutboxLockProvider{TDbContext}"/>.</item>
///   <item>SQL Server — use <see cref="SqlServerOutboxLockProvider{TDbContext}"/>.</item>
///   <item>Single-instance / testing — use the built-in default registered by
///   <c>UseOutbox&lt;TDbContext&gt;()</c> when no explicit provider is configured.</item>
/// </list>
/// </para>
/// </summary>
public interface IOutboxLockProvider<in TDbContext> where TDbContext : DbContext
{
    /// <summary>
    /// Claims and locks the next outbox group for processing.
    /// Must be called within an active database transaction (started by the caller).
    /// <para>
    /// Implementations lock one <see cref="OutboxGroup"/> row and then fetch all
    /// <see cref="OutboxMessage"/> rows that reference that group.
    /// </para>
    /// </summary>
    /// <param name="context">The DbContext with an active transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// Claimed group and messages
    /// </returns>
    Task<OutboxGroup?> TryFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken);
}
