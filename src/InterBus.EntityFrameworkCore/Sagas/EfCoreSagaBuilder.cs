using InterBus.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.EntityFrameworkCore.Sagas;

/// <summary>
/// Fluent builder for configuring the EF Core saga repository for a specific
/// <typeparamref name="TSaga"/> / <typeparamref name="TState"/> / <typeparamref name="TDbContext"/>
/// combination.
/// <para>
/// Use the extension methods on this builder to select a database-specific locking strategy
/// for multi-instance deployments:
/// <code>
/// saga.UseEfCoreRepository&lt;AppDbContext&gt;(repo => repo
///     .UsePostgreSql());
/// </code>
/// </para>
/// </summary>
/// <typeparam name="TSaga">The saga type.</typeparam>
/// <typeparam name="TState">The saga state type.</typeparam>
/// <typeparam name="TDbContext">The application's <see cref="DbContext"/>.</typeparam>
public sealed class EfCoreSagaBuilder<TSaga, TState, TDbContext>
    where TSaga : class, ISaga<TState>
    where TState : class, new()
    where TDbContext : DbContext
{
    /// <summary>The underlying service collection, used by lock-provider extension methods.</summary>
    public IServiceCollection Services { get; }

    internal EfCoreSagaBuilder(IServiceCollection services)
    {
        Services = services;
    }
}
