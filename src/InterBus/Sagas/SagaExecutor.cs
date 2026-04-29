namespace InterBus.Sagas;

/// <summary>
/// Static helper invoked by source-generated saga dispatch delegates.
/// Handles the correlation-based locking, state loading/creation, execution, and persistence
/// lifecycle in a single place so that no runtime reflection is required.
/// </summary>
public static class SagaExecutor
{
    /// <summary>
    /// Executes a single saga message handler:
    /// <list type="number">
    ///   <item>Resolves the correlation ID from the message.</item>
    ///   <item>Acquires the per-correlation-ID exclusive lock from the repository.</item>
    ///   <item>Loads existing state. If the saga is already completed, the message is dropped.</item>
    ///   <item>Creates a fresh state instance if no state exists yet (lock is already held,
    ///       so there is no race on first creation).</item>
    ///   <item>Invokes <see cref="ISagaConsumer{TState,TMessage}.Consume"/>.</item>
    ///   <item>Persists the (potentially mutated) state, marks the saga as completed, or
    ///       deletes the instance depending on which lifecycle method was called.</item>
    ///   <item>Releases the lock.</item>
    /// </list>
    /// </summary>
    public static async Task ExecuteAsync<TSaga, TState, TMessage>(
        TSaga saga,
        ISagaRepository<TState> repository,
        SagaOptions<TSaga>? options,
        TMessage message,
        IConsumeContext context,
        CancellationToken cancellationToken)
        where TSaga : class, ISagaConsumer<TState, TMessage>
        where TState : class, new()
        where TMessage : class
    {
        var correlationId = saga.GetCorrelationId(message);

        await using var sagaLock = await repository.AcquireLockAsync(correlationId, cancellationToken);

        var entry = await repository.GetAsync(correlationId, cancellationToken);

        // If the saga has already completed, drop the incoming message.
        if (entry?.IsCompleted == true)
            return;

        // Use existing state, or create a fresh instance. The lock is already held at this
        // point, so there is no race on first creation.
        var state = entry?.State ?? new TState();

        var sagaCtx = new SagaContext<TState>(state, context);

        await saga.Consume(message, sagaCtx, cancellationToken);

        if (sagaCtx.IsDeleted)
            await repository.DeleteAsync(correlationId, cancellationToken);
        else if (sagaCtx.IsCompleted)
            await repository.CompleteAsync(correlationId, sagaCtx.State, options?.TimeToLive, cancellationToken);
        else
            await repository.SaveAsync(correlationId, sagaCtx.State, options?.TimeToLive, cancellationToken);
    }
}
