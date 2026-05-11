namespace InterBus.Sagas;

/// <summary>
/// Provides persistent storage and exclusive per-instance locking for saga state.
/// Each saga state type uses its own <see cref="ISagaRepository{TSaga, TState}"/> registration.
/// </summary>
/// <remarks>
/// Implementations are responsible for serialized access to the same saga instance.
/// Use <see cref="AcquireLockAsync"/> to obtain an exclusive lock before reading or writing
/// state so that concurrent messages for the same correlation ID are processed sequentially.
/// </remarks>
/// <typeparam name="TSaga">The saga type</typeparam>
/// <typeparam name="TState">The saga state type</typeparam>
public interface ISagaRepository<TSaga, TState> 
    where TSaga : ISaga<TState>
    where TState : class, new()
{
    /// <summary>
    /// Acquires an exclusive lock for the given <paramref name="correlationId"/>, preventing
    /// concurrent processing of messages for the same saga instance.
    /// Dispose the returned <see cref="IAsyncDisposable"/> to release the lock.
    /// </summary>
    Task<IAsyncDisposable> AcquireLockAsync(string correlationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the saga entry for the given <paramref name="correlationId"/>,
    /// or <c>null</c> if no instance exists (or it has expired).
    /// The returned <see cref="SagaEntry{TState}"/> includes the state and whether
    /// the saga has been completed.
    /// </summary>
    Task<SagaEntry<TState>?> GetAsync(string correlationId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists the given <paramref name="entry"/> for the saga identified by
    /// <paramref name="correlationId"/>. The entry carries the state, the completed flag,
    /// and an optional <see cref="SagaEntry{TState}.ExpiresAtUtc"/> that the repository
    /// should honour on a best-effort basis.
    /// </summary>
    /// <remarks>
    /// This method is always called before <see cref="DeleteAsync"/> so that repositories
    /// can capture the final state for archiving or audit before the entry is removed.
    /// </remarks>
    Task SaveAsync(string correlationId, SagaEntry<TState> entry,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the saga state for the given <paramref name="correlationId"/>.
    /// A no-op if the instance does not exist.
    /// </summary>
    /// <remarks>
    /// <see cref="SaveAsync"/> is always called before this method, giving the repository an
    /// opportunity to capture the final state (e.g. for archiving).
    /// </remarks>
    Task DeleteAsync(string correlationId, CancellationToken cancellationToken = default);
}
