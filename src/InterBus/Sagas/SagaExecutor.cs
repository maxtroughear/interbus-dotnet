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
    ///   <item>Loads existing state, or creates and persists a fresh instance so that any
    ///       external actions performed inside <see cref="ISagaConsumer{TState,TMessage}.Consume"/>
    ///       can already observe a valid saga state entry.</item>
    ///   <item>Invokes <see cref="ISagaConsumer{TState,TMessage}.Consume"/>.</item>
    ///   <item>Persists the (potentially mutated) state, or deletes the instance if
    ///       <see cref="ISagaContext{TState}.Delete"/> was called.</item>
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

        var state = await repository.GetAsync(correlationId, cancellationToken);
        if (state == null)
        {
            // Persist an empty state immediately so that any messages published from within
            // Consume can observe a valid saga entry, avoiding a race on first creation.
            state = new TState();
            await repository.SaveAsync(correlationId, state, options?.TimeToLive, cancellationToken);
        }

        var sagaCtx = new SagaContext<TState>(state, context);

        await saga.Consume(message, sagaCtx, cancellationToken);

        if (sagaCtx.IsDeleted)
            await repository.DeleteAsync(correlationId, cancellationToken);
        else
            await repository.SaveAsync(correlationId, sagaCtx.State, options?.TimeToLive, cancellationToken);
    }
}
