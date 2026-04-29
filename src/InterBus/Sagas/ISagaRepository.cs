namespace InterBus.Sagas;

/// <summary>
/// Provides persistent storage and exclusive per-instance locking for saga state.
/// Each saga state type uses its own <see cref="ISagaRepository{TState}"/> registration.
/// </summary>
/// <remarks>
/// Implementations are responsible for serialized access to the same saga instance.
/// Use <see cref="AcquireLockAsync"/> to obtain an exclusive lock before reading or writing
/// state so that concurrent messages for the same correlation ID are processed sequentially.
/// </remarks>
/// <typeparam name="TState">The saga state type.</typeparam>
public interface ISagaRepository<TState> where TState : class, new()
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
    /// The returned <see cref="SagaEntry{TState}"/> includes both the state and whether
    /// the saga has been completed.
    /// </summary>
    Task<SagaEntry<TState>?> GetAsync(string correlationId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists the saga state for the given <paramref name="correlationId"/> as an active instance.
    /// </summary>
    /// <param name="correlationId">The saga instance identifier.</param>
    /// <param name="state">The state to save.</param>
    /// <param name="timeToLive">
    /// Optional time-to-live for this saga instance. Repository implementations should
    /// expire the stored state after this duration if possible. <c>null</c> means no expiry.
    /// </param>
    /// <param name="cancellationToken">A cancellation token.</param>
    Task SaveAsync(string correlationId, TState state, TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists the saga state for the given <paramref name="correlationId"/> and marks the
    /// saga as completed. Completed sagas preserve their state but will no longer process new
    /// messages.
    /// </summary>
    Task CompleteAsync(string correlationId, TState state, TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the saga state for the given <paramref name="correlationId"/>.
    /// A no-op if the instance does not exist.
    /// </summary>
    Task DeleteAsync(string correlationId, CancellationToken cancellationToken = default);
}
