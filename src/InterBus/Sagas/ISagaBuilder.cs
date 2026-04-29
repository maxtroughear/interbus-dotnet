namespace InterBus.Sagas;

/// <summary>
/// Fluent builder for configuring a registered saga.
/// Extends <see cref="IConsumerBuilder"/> so that all consumer-level options
/// (concurrency, middleware) are available alongside saga-specific settings.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public interface ISagaBuilder<TState> : IConsumerBuilder
    where TState : class, new()
{
    /// <summary>
    /// Sets the time-to-live for saga instances. Repository implementations will attempt to
    /// expire stored state after this duration. The default is no expiry.
    /// </summary>
    ISagaBuilder<TState> WithTimeToLive(TimeSpan timeToLive);
}
