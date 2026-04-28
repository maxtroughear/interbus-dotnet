using System.Collections.Concurrent;

namespace InterBus.Sagas;

/// <summary>
/// Singleton service that manages per-(saga type, correlation ID) <see cref="SemaphoreSlim"/> instances.
/// Ensures that only one message per saga instance is processed at a time, while allowing
/// messages for different correlation IDs to be processed concurrently.
/// </summary>
/// <remarks>
/// The lock dictionary grows as new correlation IDs are encountered and does not remove entries
/// automatically. In long-running applications processing a high number of unique correlation IDs,
/// consider registering a custom <see cref="ISagaRepository{TState}"/> that signals completed
/// sagas, or periodically sweeping stale entries. For typical workloads where the set of active
/// saga instances is bounded, this is not a concern.
/// </remarks>
internal sealed class SagaInstanceLockManager
{
    private readonly ConcurrentDictionary<(Type SagaType, string CorrelationId), SemaphoreSlim> _locks = new();

    /// <summary>
    /// Returns the semaphore for the given saga type and correlation ID.
    /// A new semaphore is created the first time a correlation ID is seen.
    /// </summary>
    public SemaphoreSlim GetLock(Type sagaType, string correlationId) =>
        _locks.GetOrAdd((sagaType, correlationId), static _ => new SemaphoreSlim(1, 1));
}
