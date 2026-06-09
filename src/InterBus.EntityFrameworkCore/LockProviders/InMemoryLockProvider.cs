using System.Collections.Concurrent;

namespace InterBus.EntityFrameworkCore.LockProviders;

/// <summary>
/// Default <see cref="ILockProvider"/> that uses in-process
/// <see cref="SemaphoreSlim"/> instances for per-correlation-ID exclusive locking.
/// Safe for single-instance deployments and integration tests, but does not prevent
/// concurrent access across multiple process instances.
/// For multi-instance deployments, use <see cref="PostgreSqlLockProvider{TDbContext}"/>
/// or <see cref="SqlServerLockProvider{TDbContext}"/>.
/// </summary>
internal sealed class InMemoryLockProvider : ILockProvider
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public async Task<IAsyncDisposable> AcquireLockAsync(string lockKey, CancellationToken cancellationToken = default)
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
