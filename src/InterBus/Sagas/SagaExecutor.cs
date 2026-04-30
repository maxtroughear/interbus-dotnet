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
public sealed partial class SagaExecutor<TSaga, TState>(
    ISagaRepository<TSaga, TState> repository,
    SagaOptions<TSaga> options,
    ILogger<SagaExecutor<TSaga, TState>> logger)
    where TSaga : class, ISaga<TState>
    where TState : class, new()
{
    /// <summary>
    /// Executes a single saga message handler:
    /// <list type="number">
    ///   <item>Resolves the correlation ID from the message</item>
    ///   <item>Acquires the per-correlation-ID exclusive lock from the repository</item>
    ///   <item>Loads existing state. If the saga is already completed, the message is dropped (logged at <see cref="LogLevel.Debug"/>)</item>
    ///   <item>Creates a fresh state instance if no state exists yet (lock is already held, so there is no race on first creation)</item>
    ///   <item>Invokes <see cref="ISagaConsumer{TState,TMessage}.Consume"/></item>
    ///   <item>Saves the entry (which carries the completion flag and absolute expiry) so that the repository can capture the final state before any deletion (e.g. for archiving)</item>
    ///   <item>If <see cref="ISagaContext{TState}.Delete"/> was called, also invokes <see cref="ISagaRepository{TSaga, TState}.DeleteAsync"/></item>
    ///   <item>Releases the lock</item>
    /// </list>
    /// </summary>
    public async Task ExecuteAsync<TSagaConsumer, TMessage>(
        TSagaConsumer saga,
        TMessage message,
        IConsumeContext context,
        CancellationToken cancellationToken)
        where TSagaConsumer : TSaga, ISagaConsumer<TState, TMessage>
        where TMessage : class
    {
        var correlationId = saga.GetCorrelationId(message);

        await using var sagaLock = await repository.AcquireLockAsync(correlationId, cancellationToken);

        var entry = await repository.GetAsync(correlationId, cancellationToken);

        if (entry?.IsCompleted is true)
        {
            LogSagaCompleted(typeof(TMessage).Name, typeof(TSagaConsumer).Name, correlationId);
            return;
        }

        entry ??= new SagaEntry<TState>(
            new TState(),
            ExpiresAtUtc: options.TimeToLive.HasValue
                ? DateTimeOffset.UtcNow.Add(options.TimeToLive.Value)
                : null);

        var sagaCtx = new SagaContext<TState>(entry, context);

        await saga.Consume(message, sagaCtx, cancellationToken);

        await repository.SaveAsync(correlationId, entry, cancellationToken);

        if (entry.IsDeleted)
            await repository.DeleteAsync(correlationId, cancellationToken);
    }

    [LoggerMessage(LogLevel.Debug,
        "Skipping {MessageType} for completed saga {SagaType} (correlationId: {CorrelationId})")]
    partial void LogSagaCompleted(string messageType, string sagaType, string correlationId);
}
