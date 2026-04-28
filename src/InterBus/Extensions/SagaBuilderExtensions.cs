using System.Diagnostics.CodeAnalysis;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Extensions;

/// <summary>Extension methods for registering sagas with <see cref="IInterBusBuilder"/>.</summary>
public static class SagaBuilderExtensions
{
    /// <summary>
    /// Registers a saga with the InterBus infrastructure.
    /// <para>
    /// <typeparamref name="TSaga"/> must implement <see cref="ISagaConsumer{TState,TMessage}"/> for
    /// each message type it handles. The saga's queue is derived from the saga type name by convention.
    /// </para>
    /// <para>
    /// A default <see cref="InMemorySagaRepository{TState}"/> is registered as a singleton if no
    /// <see cref="ISagaRepository{TState}"/> has been registered already. Override it by registering
    /// your own <see cref="ISagaRepository{TState}"/> before calling this method.
    /// </para>
    /// </summary>
    /// <typeparam name="TSaga">
    /// The saga type. Must implement <see cref="ISaga{TState}"/> and at least one
    /// <see cref="ISagaConsumer{TState,TMessage}"/>.
    /// </typeparam>
    /// <typeparam name="TState">The saga state type. Must be a class with a public parameterless constructor.</typeparam>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure the saga (e.g. set a TTL or concurrency count).
    /// </param>
    public static IInterBusBuilder AddSaga<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
        TSaga,
        TState>(
        this IInterBusBuilder builder,
        Action<ISagaBuilder>? configure = null)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        // Register infrastructure services shared across all sagas.
        builder.Services.TryAddSingleton<SagaInstanceLockManager>();
        builder.Services.TryAddSingleton<ISagaRepository<TState>, InMemorySagaRepository<TState>>();

        // Run the configure delegate first so the TTL and concurrency are known before
        // the dispatch delegate is built.
        var sagaBuilder = new SagaBuilder<TSaga, TState>();
        configure?.Invoke(sagaBuilder);

        // Build the dispatch delegate using the resolved TTL.
        var (messageTypes, dispatch) = SagaDispatchHelper.Build<TSaga, TState>(sagaBuilder.TimeToLive);

        // Register the saga as a consumer, forwarding the concurrency setting if provided.
        builder.AddConsumer<TSaga>(messageTypes, dispatch, sagaBuilder.Concurrency.HasValue
            ? cb => cb.WithConcurrency(sagaBuilder.Concurrency.Value)
            : null);

        return builder;
    }
}
