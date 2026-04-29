using System.Diagnostics.CodeAnalysis;
using InterBus.Consuming;
using InterBus.Publishing;
using InterBus.Sending;
using Microsoft.Extensions.DependencyInjection;

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
    /// Registers <see cref="InMemorySagaRepository{TState}"/> as the
    /// <see cref="ISagaRepository{TState}"/> for this saga. A no-op if an
    /// <see cref="ISagaRepository{TState}"/> has already been registered.
    /// </summary>
    ISagaBuilder<TState> UseInMemoryRepository();

    /// <summary>
    /// Sets the time-to-live for saga instances. Repository implementations will attempt to
    /// expire stored state after this duration. The default is no expiry.
    /// </summary>
    ISagaBuilder<TState> WithTimeToLive(TimeSpan timeToLive);
}
