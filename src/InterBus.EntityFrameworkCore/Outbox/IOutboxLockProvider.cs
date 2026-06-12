using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Provides the database-specific strategy for claiming an outbox shard and fetching the next
/// pending group within it, so that concurrent processor instances deliver messages in FIFO order.
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
    /// Ensures one <see cref="OutboxLock"/> row exists for each shard in the database.
    /// Called once at processor startup before the processing loop begins.
    /// The default implementation is a no-op (suitable for the default EF Core provider).
    /// </summary>
    Task EnsureShardLocksAsync(TDbContext context, int shardCount, CancellationToken cancellationToken)
        => Task.CompletedTask;

    /// <summary>
    /// Atomically claims a shard and returns the oldest pending <see cref="OutboxGroup"/> within
    /// it, or <c>null</c> if no pending work is available.
    /// Must be called within an active database transaction (started by the caller).
    /// <para>
    /// Implementations should lock one <see cref="OutboxLock"/> shard row and then fetch the
    /// oldest <see cref="OutboxGroup"/> (and its <see cref="OutboxMessage"/> rows) in that shard,
    /// ordered by <see cref="OutboxGroup.Id"/> ascending to guarantee FIFO delivery.
    /// </para>
    /// </summary>
    /// <param name="context">The DbContext with an active transaction.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="preferredStartShard">
    /// Preferred shard index to start round-robin selection from. Implementations should treat
    /// this as a fairness hint only.
    /// </param>
    /// <returns>
    /// The claimed shard index and oldest pending group, or <c>null</c> when no work is available.
    /// </returns>
    Task<(int Shard, OutboxGroup Group)?> TryClaimAndFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken,
        int preferredStartShard = 0);
}
