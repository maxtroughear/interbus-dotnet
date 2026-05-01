namespace InterBus.Sagas;

/// <summary>
/// Represents a stored saga instance, including its state, lifecycle status, and optional expiry.
/// Returned by <see cref="ISagaRepository{TSaga, TState}.GetAsync"/> so callers can distinguish
/// between a saga that does not exist (<c>null</c>), an active saga, and a completed one.
/// Passed to <see cref="ISagaRepository{TSaga, TState}.SaveAsync"/> so the repository receives
/// the full entry in a single call.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
/// <param name="State">The saga instance state.</param>
/// <param name="ExpiresAtUtc">
/// The pre-computed absolute UTC time at which this saga entry expires, or <c>null</c> for no expiry.
/// Repository implementations should remove (or treat as absent) entries whose
/// <see cref="ExpiresAtUtc"/> is in the past. Computed by the caller before calling
/// <see cref="ISagaRepository{TSaga, TState}.SaveAsync"/>.
/// </param>
public sealed record SagaEntry<TState>(TState State, DateTimeOffset? ExpiresAtUtc = null)
    where TState : class, new()
{
    /// <summary>
    /// <c>true</c> if the saga has been completed via <see cref="ISagaContext{TState}.Complete"/>.
    /// Completed sagas preserve their state but no longer process new messages.
    /// </summary>
    public bool IsCompleted { get; set; }

    /// <summary>
    /// <c>true</c> if the saga has been marked for deletion via <see cref="ISagaContext{TState}.Delete"/>
    /// Deleted sagas are removed from the repository and no longer process new messages. In some cases they may be
    /// archived instead or moved.
    /// </summary>
    public bool IsDeleted { get; set; }
}
