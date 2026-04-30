using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Sagas;

/// <summary>
/// Default <see cref="ISagaLockProvider{TDbContext}"/> that uses in-process
/// <see cref="SemaphoreSlim"/> instances for per-correlation-ID exclusive locking.
/// Safe for single-instance deployments and integration tests, but does not prevent
/// concurrent access across multiple process instances.
/// For multi-instance deployments, use <see cref="PostgreSqlSagaLockProvider{TDbContext}"/>
/// or <see cref="SqlServerSagaLockProvider{TDbContext}"/>.
/// </summary>
internal sealed class DefaultSagaLockProvider<TDbContext> : ISagaLockProvider<TDbContext>
    where TDbContext : DbContext
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public async Task<IAsyncDisposable> AcquireLockAsync(TDbContext context, string lockKey,
        CancellationToken cancellationToken = default)
    {
        var semaphore = _locks.GetOrAdd(lockKey, static _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync(cancellationToken);
        return new SemaphoreReleaser(semaphore);
    }

    private sealed class SemaphoreReleaser(SemaphoreSlim semaphore) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            semaphore.Release();
            return ValueTask.CompletedTask;
        }
    }
}
