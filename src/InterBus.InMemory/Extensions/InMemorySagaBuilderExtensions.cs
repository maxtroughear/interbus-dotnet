using InterBus.InMemory.Sagas;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.InMemory.Extensions;

/// <summary>
/// Extension methods for configuring saga repositories on an <see cref="ISagaBuilder{TState}"/>.
/// </summary>
public static class InMemorySagaBuilderExtensions
{
    /// <summary>
    /// Registers <see cref="InMemorySagaRepository{TState}"/> as the
    /// <see cref="ISagaRepository{TState}"/> for this saga. A no-op if an
    /// <see cref="ISagaRepository{TState}"/> has already been registered.
    /// </summary>
    public static ISagaBuilder<TState> UseInMemoryRepository<TState>(
        this ISagaBuilder<TState> builder)
        where TState : class, new()
    {
        builder.Services.TryAddSingleton<ISagaRepository<TState>, InMemorySagaRepository<TState>>();
        return builder;
    }
}
