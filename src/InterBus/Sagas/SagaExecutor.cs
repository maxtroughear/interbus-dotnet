using Microsoft.Extensions.Logging;

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
    ///   <item>Loads existing state. If the saga is already completed, the message is dropped
    ///       (logged at <see cref="LogLevel.Debug"/>).</item>
    ///   <item>Creates a fresh state instance if no state exists yet (lock is already held,
    ///       so there is no race on first creation).</item>
    ///   <item>Invokes <see cref="ISagaConsumer{TState,TMessage}.Consume"/>.</item>
    ///   <item>Saves the entry (which carries the completion flag and TTL) so that the
    ///       repository can capture the final state before any deletion (e.g. for archiving).</item>
    ///   <item>If <see cref="ISagaContext{TState}.Delete"/> was called, also invokes
    ///       <see cref="ISagaRepository{TState}.DeleteAsync"/>.</item>
    ///   <item>Releases the lock.</item>
    /// </list>
    /// </summary>
    public static async Task ExecuteAsync<TSaga, TState, TMessage>(
        TSaga saga,
        ISagaRepository<TState> repository,
        SagaOptions<TSaga>? options,
        TMessage message,
        IConsumeContext context,
        ILogger logger,
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
        {
            logger.LogDebug(
                "Skipping {MessageType} for completed saga {SagaType} (correlationId: {CorrelationId})",
                typeof(TMessage).Name, typeof(TSaga).Name, correlationId);
            return;
        }

        // Use existing state, or create a fresh instance. The lock is already held at this
        // point, so there is no race on first creation.
        var state = entry?.State ?? new TState();

        var sagaCtx = new SagaContext<TState>(state, context);

        await saga.Consume(message, sagaCtx, cancellationToken);

        // Always save first so the repository can capture the final state before any deletion
        // (e.g. for archiving or audit).  The entry carries the IsCompleted flag and TTL.
        var savedEntry = new SagaEntry<TState>(sagaCtx.State, sagaCtx.IsCompleted, options?.TimeToLive);
        await repository.SaveAsync(correlationId, savedEntry, cancellationToken);

        if (sagaCtx.IsDeleted)
            await repository.DeleteAsync(correlationId, cancellationToken);
    }
}
