namespace InterBus.Sagas;

/// <summary>
/// Provides access to the current saga instance state and saga lifecycle operations.
/// An instance is passed directly to <see cref="ISagaConsumer{TState,TMessage}.Consume"/> for each message dispatch.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public interface ISagaContext<TState> : IConsumeContext
    where TState : class, new()
{
    /// <summary>
    /// The current saga instance state. Modifications made to this object will be persisted
    /// after the <see cref="ISagaConsumer{TState,TMessage}.Consume"/> method completes,
    /// unless <see cref="Delete"/> has been called.
    /// </summary>
    TState State { get; }

    /// <summary>
    /// Marks this saga instance for deletion. The saga state will be removed from the repository
    /// after the current <see cref="ISagaConsumer{TState,TMessage}.Consume"/> method completes.
    /// </summary>
    void Delete();

    /// <summary>
    /// Marks this saga instance as completed. The saga state is preserved in the repository
    /// but no further messages will be processed by this saga instance. Any subsequent
    /// messages targeting this correlation ID are silently dropped.
    /// </summary>
    void Complete();
}
