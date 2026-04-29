namespace InterBus.Sagas;

/// <summary>
/// Concrete <see cref="ISagaContext{TState}"/> passed to <see cref="ISagaConsumer{TState,TMessage}.Consume"/>.
/// Wraps the inner <see cref="IConsumeContext"/> and tracks whether the saga has been marked for deletion or completion.
/// </summary>
internal sealed class SagaContext<TState>(TState state, IConsumeContext inner)
    : ISagaContext<TState>
    where TState : class, new()
{
    private bool _deleted;
    private bool _completed;

    /// <inheritdoc/>
    public TState State { get; } = state;

    /// <inheritdoc/>
    public MessageHeaders Headers => inner.Headers;

    /// <inheritdoc/>
    public string? CorrelationId => inner.CorrelationId;

    /// <inheritdoc/>
    public void Delete() => _deleted = true;

    /// <inheritdoc/>
    public void Complete() => _completed = true;

    /// <summary>Returns <c>true</c> if <see cref="Delete"/> has been called.</summary>
    internal bool IsDeleted => _deleted;

    /// <summary>Returns <c>true</c> if <see cref="Complete"/> has been called.</summary>
    internal bool IsCompleted => _completed;
}
