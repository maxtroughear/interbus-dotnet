using InterBus.EntityFrameworkCore.Sagas;
using InterBus.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the EF Core saga repository on an <see cref="SagaBuilder{TSaga, TState}"/>.</summary>
public static class EfCoreSagaBuilderExtensions
{
    /// <summary>
    /// Registers <see cref="EfCoreSagaRepository{TSaga, TState, TDbContext}"/> as the
    /// <see cref="ISagaRepository{TSaga, TState}"/> for this saga, persisting state to the
    /// supplied <typeparamref name="TDbContext"/>.
    /// <para>
    /// <typeparamref name="TDbContext"/> must call <c>modelBuilder.AddInterBusSagas()</c>
    /// inside <c>OnModelCreating</c>.
    /// </para>
    /// <para>
    /// By default, an in-process lock is used (safe for single-instance deployments). For
    /// multi-instance deployments, select a database-specific lock strategy via the
    /// <paramref name="configure"/> delegate:
    /// <code>
    /// saga.UseEfCoreRepository&lt;MySaga, MyState, AppDbContext&gt;(repo => repo.UsePostgreSql());
    /// </code>
    /// </para>
    /// </summary>
    /// <typeparam name="TSaga">The saga type.</typeparam>
    /// <typeparam name="TState">The saga state type.</typeparam>
    /// <typeparam name="TDbContext">The application's <see cref="DbContext"/>.</typeparam>
    /// <param name="builder">The saga builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure the <see cref="EfCoreSagaBuilder{TSaga, TState, TDbContext}"/>,
    /// e.g. to select a database-specific lock provider.
    /// </param>
    public static SagaBuilder<TSaga, TState> UseEfCoreRepository<TSaga, TState, TDbContext>(
        this SagaBuilder<TSaga, TState> builder,
        Action<EfCoreSagaBuilder<TSaga, TState, TDbContext>>? configure = null)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Register the default lock provider; the configure delegate may replace it.
        builder.Services.TryAddSingleton<
            ISagaLockProvider<TDbContext>,
            DefaultSagaLockProvider<TDbContext>>();

        var sagaBuilder = new EfCoreSagaBuilder<TSaga, TState, TDbContext>(builder.Services);
        configure?.Invoke(sagaBuilder);

        builder.Services.TryAddScoped<
            ISagaRepository<TSaga, TState>,
            EfCoreSagaRepository<TSaga, TState, TDbContext>>();

        return builder;
    }
}
