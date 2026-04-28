namespace InterBus.Sagas;

/// <summary>
/// Provides persistent storage for saga state instances.
/// Each saga type uses its own <see cref="ISagaRepository{TState}"/> instance.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public interface ISagaRepository<TState> where TState : class, new()
{
    /// <summary>
    /// Retrieves the saga state for the given <paramref name="correlationId"/>,
    /// or <c>null</c> if no instance exists (or it has expired).
    /// </summary>
    Task<TState?> GetAsync(string correlationId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists the saga state for the given <paramref name="correlationId"/>.
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
    /// Removes the saga state for the given <paramref name="correlationId"/>.
    /// A no-op if the instance does not exist.
    /// </summary>
    Task DeleteAsync(string correlationId, CancellationToken cancellationToken = default);
}
