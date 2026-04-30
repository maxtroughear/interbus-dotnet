namespace InterBus.Sagas;

/// <summary>
/// Concrete <see cref="ISagaContext{TState}"/> passed to <see cref="ISagaConsumer{TState,TMessage}.Consume"/>.
/// Wraps the inner <see cref="IConsumeContext"/> and tracks whether the saga has been marked for deletion or completion.
/// </summary>
internal sealed class SagaContext<TState>(SagaEntry<TState> entry, IConsumeContext inner)
    : ISagaContext<TState>
    where TState : class, new()
{
    /// <inheritdoc/>
    public TState State { get; } = entry.State;

    /// <inheritdoc/>
    public MessageHeaders Headers => inner.Headers;

    /// <inheritdoc/>
    public string? CorrelationId => inner.CorrelationId;

    /// <inheritdoc/>
    public void Delete() => entry.IsDeleted = true;

    /// <inheritdoc/>
    public void Complete() => entry.IsCompleted = true;
}
