using InterBus.EntityFrameworkCore.LockProviders;
using InterBus.EntityFrameworkCore.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

public class EfCoreBuilder<TDbContext> where TDbContext : DbContext
{
    private IServiceCollection Services { get; }

    internal EfCoreBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Configures the saga repository to use the default in-process lock provider.
    /// Provides per-correlation-ID locking via <see cref="System.Threading.SemaphoreSlim"/> —
    /// safe for single-instance deployments and integration tests.
    /// This is the default and does not need to be called explicitly.
    /// </summary>
    public EfCoreBuilder<TDbContext> UseDefaultLockProvider()
    {
        Services.Replace(ServiceDescriptor.Singleton<ILockProvider, InMemoryLockProvider>());
        return this;
    }

    /// <summary>
    /// Configures the saga repository to use the PostgreSQL lock provider.
    /// Uses <c>pg_advisory_xact_lock</c> (transaction-scoped) to acquire a distributed exclusive lock
    /// per correlation ID. Recommended for multi-instance deployments targeting PostgreSQL.
    /// </summary>
    public EfCoreBuilder<TDbContext> UsePostgreSql()
    {
        Services.Replace(ServiceDescriptor.Scoped<ILockProvider, PostgreSqlLockProvider<TDbContext>>());
        return this;
    }

    /// <summary>
    /// Configures the saga repository to use the SQL Server lock provider.
    /// Uses <c>sp_getapplock</c> (transaction-scoped) to acquire a distributed exclusive lock
    /// per correlation ID. Recommended for multi-instance deployments targeting SQL Server.
    /// </summary>
    public EfCoreBuilder<TDbContext> UseSqlServer()
    {
        Services.Replace(ServiceDescriptor.Scoped<ILockProvider, SqlServerLockProvider<TDbContext>>());
        return this;
    }
}
