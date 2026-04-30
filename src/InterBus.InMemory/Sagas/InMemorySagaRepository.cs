using System.Collections.Concurrent;
using InterBus.Sagas;

namespace InterBus.InMemory.Sagas;

/// <summary>
/// In-memory implementation of <see cref="ISagaRepository{TState}"/>.
/// State is held in a <see cref="ConcurrentDictionary{TKey,TValue}"/> and is lost when the
/// process restarts. Lazy TTL expiry is performed on each read using the
/// <see cref="SagaEntry{TState}.ExpiresAtUtc"/> field stored directly in the entry.
/// Per-correlation-ID locking is handled with <see cref="SemaphoreSlim"/> instances so that
/// only one message per saga instance is processed at a time.
/// </summary>
/// <remarks>
/// The internal lock dictionary grows as new correlation IDs are encountered and entries are
/// not removed automatically. In long-running applications processing a high volume of unique
/// correlation IDs, memory growth may occur. Use <see cref="ISagaContext{TState}.Delete"/> to
/// delete completed saga instances, which removes the state entry; lock entries are lightweight
/// and bounded by the number of concurrently active correlation IDs for typical workloads.
/// </remarks>
public sealed class InMemorySagaRepository<TState> : ISagaRepository<TState>
    where TState : class, new()
{
    private readonly ConcurrentDictionary<string, SagaEntry<TState>> _store = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public async Task<IAsyncDisposable> AcquireLockAsync(string correlationId,
        CancellationToken cancellationToken = default)
    {
        var semaphore = _locks.GetOrAdd(correlationId, static _ => new SemaphoreSlim(1, 1));
        await semaphore.WaitAsync(cancellationToken);
        return new SemaphoreReleaser(semaphore);
    }

    /// <inheritdoc/>
    public Task<SagaEntry<TState>?> GetAsync(string correlationId,
        CancellationToken cancellationToken = default)
    {
        if (_store.TryGetValue(correlationId, out var entry))
        {
            if (entry.ExpiresAtUtc.HasValue && entry.ExpiresAtUtc.Value <= DateTimeOffset.UtcNow)
            {
                _store.TryRemove(correlationId, out _);
                return Task.FromResult<SagaEntry<TState>?>(null);
            }

            return Task.FromResult<SagaEntry<TState>?>(entry);
        }

        return Task.FromResult<SagaEntry<TState>?>(null);
    }

    /// <inheritdoc/>
    public Task SaveAsync(string correlationId, SagaEntry<TState> entry,
        CancellationToken cancellationToken = default)
    {
        _store[correlationId] = entry;
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        _store.TryRemove(correlationId, out _);
        return Task.CompletedTask;
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
