using System.Diagnostics;
using InterBus.Consuming;
using InterBus.Telemetry;
using Microsoft.Extensions.Logging;

namespace InterBus.Sagas;

/// <summary>
/// DI-registered saga executor responsible for the correlation-based locking, state
/// loading/creation, execution, and persistence lifecycle for a specific
/// <typeparamref name="TSaga"/> / <typeparamref name="TState"/> pair.
/// </summary>
/// <typeparam name="TSaga">The saga type</typeparam>
/// <typeparam name="TState">The saga state type</typeparam>
/// <typeparam name="TMessage">The message type</typeparam>
public sealed partial class SagaExecutor<TSaga, TState, TMessage>(
    TSaga saga,
    ISagaRepository<TSaga, TState> repository,
    SagaOptions<TSaga> options,
    IInterBusDiagnostics diagnostics,
    ILogger<SagaExecutor<TSaga, TState, TMessage>> logger)
    : IConsumerExecutor<TSaga, TMessage>
    where TSaga : ISagaConsumer<TState, TMessage>
    where TState : class, new()
    where TMessage : class
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
    public async Task ExecuteAsync(
        ConsumeContext<TMessage> context,
        CancellationToken cancellationToken)
    {
        var correlationId = saga.GetCorrelationId(context.Message);

        await using var sagaLock = await repository.AcquireLockAsync(correlationId, cancellationToken);

        var entry = await repository.GetAsync(correlationId, cancellationToken);

        if (entry?.IsCompleted is true)
        {
            LogSagaCompleted(typeof(TMessage).Name, typeof(TSaga).Name, correlationId);
            return;
        }

        entry ??= new SagaEntry<TState>(
            new TState(),
            ExpiresAtUtc: options.TimeToLive.HasValue
                ? DateTimeOffset.UtcNow.Add(options.TimeToLive.Value)
                : null);

        var sagaCtx = new SagaContext<TState>(entry, correlationId, context);

        using var activity = diagnostics.ActivitySource.StartActivity(
            $"{typeof(TSaga).Name} consume");
        try
        {
            await saga.Consume(context.Message, sagaCtx, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }

        await repository.SaveAsync(correlationId, entry, cancellationToken);

        if (entry.IsDeleted)
            await repository.DeleteAsync(correlationId, cancellationToken);
    }

    [LoggerMessage(LogLevel.Debug,
        "Skipping {MessageType} for completed saga {SagaType} (correlationId: {CorrelationId})")]
    partial void LogSagaCompleted(string messageType, string sagaType, string correlationId);
}
