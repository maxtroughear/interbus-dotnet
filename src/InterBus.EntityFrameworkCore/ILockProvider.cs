using InterBus.EntityFrameworkCore.LockProviders;

namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Provides the database-specific strategy for acquiring and releasing a distributed,
/// per-correlation-ID exclusive lock on a saga instance.
/// <para>
/// Implement this interface using the locking primitives of your database provider:
/// <list type="bullet">
///   <item>PostgreSQL — use <see cref="PostgreSqlLockProvider{TDbContext}"/>.</item>
///   <item>SQL Server — use <see cref="SqlServerLockProvider{TDbContext}"/>.</item>
///   <item>Single-instance / testing — use the built-in default registered by
///   <c>UseEfCoreRepository&lt;TDbContext&gt;()</c> when no explicit provider is configured.</item>
/// </list>
/// </para>
/// </summary>
public interface ILockProvider
{
    /// <summary>
    /// Acquires an exclusive lock identified by <paramref name="lockKey"/>.
    /// Blocks until the lock can be granted or <paramref name="cancellationToken"/> is cancelled.
    /// Dispose the returned <see cref="IAsyncDisposable"/> to release the lock.
    /// </summary>
    /// <param name="lockKey">A unique string identifying the lock resource (e.g. <c>"{SagaType}:{CorrelationId}"</c>).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IAsyncDisposable> AcquireLockAsync(string lockKey, CancellationToken cancellationToken = default);
}
