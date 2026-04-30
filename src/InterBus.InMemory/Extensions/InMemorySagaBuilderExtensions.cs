using InterBus.InMemory.Sagas;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.InMemory.Extensions;

/// <summary>
/// Extension methods for configuring saga repositories on an <see cref="ISagaBuilder{TSaga, TState}"/>.
/// </summary>
public static class InMemorySagaBuilderExtensions
{
    /// <summary>
    /// Registers <see cref="InMemorySagaRepository{TSaga, TState}"/> as the
    /// <see cref="ISagaRepository{TSaga, TState}"/> for this saga. A no-op if an
    /// <see cref="ISagaRepository{TSaga, TState}"/> has already been registered.
    /// </summary>
    public static ISagaBuilder<TSaga, TState> UseInMemoryRepository<TSaga, TState>(
        this ISagaBuilder<TSaga, TState> builder)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        builder.Services.TryAddSingleton<ISagaRepository<TSaga, TState>, InMemorySagaRepository<TSaga, TState>>();
        return builder;
    }
}
