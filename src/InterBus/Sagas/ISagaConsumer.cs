namespace InterBus.Sagas;

/// <summary>
/// Defines a saga message handler for a specific message type <typeparamref name="TMessage"/>.
/// Implement this interface (alongside <see cref="ISaga{TState}"/>) once per message type the saga handles.
/// </summary>
/// <typeparam name="TState">The shared saga state type.</typeparam>
/// <typeparam name="TMessage">The message type handled by this handler.</typeparam>
public interface ISagaConsumer<TState, TMessage> : ISaga<TState>
    where TState : class, new()
    where TMessage : class
{
    /// <summary>
    /// Returns the correlation identifier for this message.
    /// All messages with the same correlation ID are routed to the same saga instance.
    /// The returned value must be a stable, non-null string; it is used as the storage key.
    /// </summary>
    string GetCorrelationId(TMessage message);

    /// <summary>
    /// Handles the <paramref name="message"/> for this saga instance.
    /// The <paramref name="context"/> provides access to the current saga state and allows
    /// the saga to be deleted by calling <see cref="ISagaContext{TState}.Delete"/>.
    /// </summary>
    Task Consume(TMessage message, ISagaContext<TState> context,
        CancellationToken cancellationToken = default);
}
