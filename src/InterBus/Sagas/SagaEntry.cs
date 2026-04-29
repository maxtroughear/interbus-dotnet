namespace InterBus.Sagas;

/// <summary>
/// Represents a stored saga instance, including its state and lifecycle status.
/// Returned by <see cref="ISagaRepository{TState}.GetAsync"/> so callers can distinguish
/// between a saga that does not exist (<c>null</c>), an active saga, and a completed one.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
/// <param name="State">The saga instance state.</param>
/// <param name="IsCompleted">
/// <c>true</c> if the saga has been completed via <see cref="ISagaContext{TState}.Complete"/>.
/// Completed sagas preserve their state but no longer process new messages.
/// </param>
public sealed record SagaEntry<TState>(TState State, bool IsCompleted)
    where TState : class, new();
