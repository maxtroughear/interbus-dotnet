namespace InterBus.Sagas;

/// <summary>
/// Concrete <see cref="ISagaContext{TState}"/> passed to <see cref="ISagaConsumer{TState,TMessage}.Consume"/>.
/// Wraps the inner <see cref="IConsumeContext"/> and tracks whether the saga has been marked for deletion or completion.
/// </summary>
internal sealed class SagaContext<TState>(TState state, IConsumeContext inner)
    : ISagaContext<TState>
    where TState : class, new()
{
    /// <inheritdoc/>
    public TState State { get; } = state;

    /// <inheritdoc/>
    public MessageHeaders Headers => inner.Headers;

    /// <inheritdoc/>
    public string? CorrelationId => inner.CorrelationId;

    /// <inheritdoc/>
    public void Delete() => IsDeleted = true;

    /// <inheritdoc/>
    public void Complete() => IsCompleted = true;

    internal bool IsDeleted { get; private set; }

    internal bool IsCompleted { get; private set; }
}
