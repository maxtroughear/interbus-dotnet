using System.Collections.Concurrent;

namespace InterBus.Sagas;

/// <summary>
/// In-memory implementation of <see cref="ISagaRepository{TState}"/>.
/// State is held in a <see cref="ConcurrentDictionary{TKey,TValue}"/> and is lost when the process restarts.
/// TTL is respected: expired entries are removed lazily on the next read.
/// </summary>
public sealed class InMemorySagaRepository<TState> : ISagaRepository<TState>
    where TState : class, new()
{
    private readonly ConcurrentDictionary<string, Entry> _store = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public Task<TState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        if (_store.TryGetValue(correlationId, out var entry))
        {
            if (entry.ExpiresAtUtc.HasValue && entry.ExpiresAtUtc.Value <= DateTimeOffset.UtcNow)
            {
                _store.TryRemove(correlationId, out _);
                return Task.FromResult<TState?>(null);
            }

            return Task.FromResult<TState?>(entry.State);
        }

        return Task.FromResult<TState?>(null);
    }

    /// <inheritdoc/>
    public Task SaveAsync(string correlationId, TState state, TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default)
    {
        var expiresAt = timeToLive.HasValue
            ? DateTimeOffset.UtcNow.Add(timeToLive.Value)
            : (DateTimeOffset?)null;

        _store[correlationId] = new Entry(state, expiresAt);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        _store.TryRemove(correlationId, out _);
        return Task.CompletedTask;
    }

    private sealed record Entry(TState State, DateTimeOffset? ExpiresAtUtc);
}
