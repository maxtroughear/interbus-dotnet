namespace InterBus.Sagas;

/// <summary>
/// Represents a stored saga instance, including its state, lifecycle status, and optional TTL.
/// Returned by <see cref="ISagaRepository{TState}.GetAsync"/> so callers can distinguish
/// between a saga that does not exist (<c>null</c>), an active saga, and a completed one.
/// Passed to <see cref="ISagaRepository{TState}.SaveAsync"/> so the repository receives
/// the full entry in a single call.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
/// <param name="State">The saga instance state.</param>
/// <param name="IsCompleted">
/// <c>true</c> if the saga has been completed via <see cref="ISagaContext{TState}.Complete"/>.
/// Completed sagas preserve their state but no longer process new messages.
/// </param>
/// <param name="TimeToLive">
/// Optional duration after which the saga entry should be considered expired.
/// Repository implementations should honour this on a best-effort basis.
/// <c>null</c> means no expiry. Only relevant when passed to
/// <see cref="ISagaRepository{TState}.SaveAsync"/>; repositories are not required to
/// return the original value from <see cref="ISagaRepository{TState}.GetAsync"/>.
/// </param>
public sealed record SagaEntry<TState>(TState State, bool IsCompleted = false, TimeSpan? TimeToLive = null)
    where TState : class, new();
