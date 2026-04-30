using Microsoft.Extensions.Logging;

namespace InterBus.Sagas;

/// <summary>
/// DI-registered saga executor responsible for the correlation-based locking, state
/// loading/creation, execution, and persistence lifecycle for a specific
/// <typeparamref name="TSaga"/> / <typeparamref name="TState"/> pair.
/// Registered as a scoped service by <see cref="IInterBusBuilder.AddSaga{TSaga,TState}"/>;
/// dependencies are resolved from the container without any runtime reflection in the
/// hot path.
/// </summary>
/// <typeparam name="TSaga">The saga type.</typeparam>
/// <typeparam name="TState">The saga state type.</typeparam>
public sealed class SagaExecutor<TSaga, TState>(
    ISagaRepository<TState> repository,
    SagaOptions<TSaga> options,
    ILogger<SagaExecutor<TSaga, TState>> logger)
    where TSaga : class
    where TState : class, new()
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
    ///   <item>Saves the entry (which carries the completion flag and absolute expiry) so that
    ///       the repository can capture the final state before any deletion (e.g. for archiving).</item>
    ///   <item>If <see cref="ISagaContext{TState}.Delete"/> was called, also invokes
    ///       <see cref="ISagaRepository{TState}.DeleteAsync"/>.</item>
    ///   <item>Releases the lock.</item>
    /// </list>
    /// </summary>
    public async Task ExecuteAsync<TMessage>(
        TSaga saga,
        TMessage message,
        IConsumeContext context,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        var consumer = (ISagaConsumer<TState, TMessage>)saga;
        var correlationId = consumer.GetCorrelationId(message);

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

        await consumer.Consume(message, sagaCtx, cancellationToken);

        // Compute the absolute expiry from the configured TTL.
        var expiresAtUtc = options.TimeToLive.HasValue
            ? DateTimeOffset.UtcNow.Add(options.TimeToLive.Value)
            : (DateTimeOffset?)null;

        // Always save first so the repository can capture the final state before any deletion
        // (e.g. for archiving or audit). The entry carries the IsCompleted flag and expiry.
        var savedEntry = new SagaEntry<TState>(sagaCtx.State, sagaCtx.IsCompleted, expiresAtUtc);
        await repository.SaveAsync(correlationId, savedEntry, cancellationToken);

        if (sagaCtx.IsDeleted)
            await repository.DeleteAsync(correlationId, cancellationToken);
    }
}
