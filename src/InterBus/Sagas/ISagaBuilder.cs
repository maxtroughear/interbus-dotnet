namespace InterBus.Sagas;

/// <summary>
/// Fluent builder for configuring a registered saga.
/// Obtained from <see cref="Extensions.SagaBuilderExtensions.AddSaga{TSaga,TState}"/>.
/// </summary>
public interface ISagaBuilder
{
    /// <summary>
    /// Sets the time-to-live for saga instances. Repository implementations will attempt to
    /// expire stored state after this duration. The default is no expiry.
    /// </summary>
    ISagaBuilder WithTimeToLive(TimeSpan timeToLive);

    /// <summary>
    /// Sets the number of concurrent messages that can be dispatched from this saga's queue.
    /// Because each saga instance is protected by a per-correlation-ID lock, messages for
    /// different correlation IDs may be processed in parallel.
    /// Defaults to <c>1</c> (sequential queue dispatch).
    /// </summary>
    ISagaBuilder WithConcurrency(int concurrencyCount);
}
