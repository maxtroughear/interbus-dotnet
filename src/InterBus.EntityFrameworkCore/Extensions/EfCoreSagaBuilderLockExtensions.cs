using InterBus.EntityFrameworkCore.Sagas;
using InterBus.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for selecting a lock provider on <see cref="EfCoreSagaBuilder{TSaga,TState,TDbContext}"/>.</summary>
public static class EfCoreSagaBuilderLockExtensions
{
    extension<TSaga, TState, TDbContext>(EfCoreSagaBuilder<TSaga, TState, TDbContext> builder)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
        where TDbContext : DbContext
    {
        /// <summary>
        /// Configures the saga repository to use the default in-process lock provider.
        /// Provides per-correlation-ID locking via <see cref="System.Threading.SemaphoreSlim"/> —
        /// safe for single-instance deployments and integration tests.
        /// This is the default and does not need to be called explicitly.
        /// </summary>
        public EfCoreSagaBuilder<TSaga, TState, TDbContext> UseDefaultLockProvider()
        {
            builder.Services.Replace(ServiceDescriptor.Singleton<
                ISagaLockProvider<TDbContext>,
                DefaultSagaLockProvider<TDbContext>>());
            return builder;
        }

        /// <summary>
        /// Configures the saga repository to use the PostgreSQL lock provider.
        /// Uses <c>pg_advisory_lock</c> (session-scoped) to acquire a distributed exclusive lock
        /// per correlation ID. Recommended for multi-instance deployments targeting PostgreSQL.
        /// </summary>
        public EfCoreSagaBuilder<TSaga, TState, TDbContext> UsePostgreSql()
        {
            builder.Services.Replace(ServiceDescriptor.Singleton<
                ISagaLockProvider<TDbContext>,
                PostgreSqlSagaLockProvider<TDbContext>>());
            return builder;
        }

        /// <summary>
        /// Configures the saga repository to use the SQL Server lock provider.
        /// Uses <c>sp_getapplock</c> (session-scoped) to acquire a distributed exclusive lock
        /// per correlation ID. Recommended for multi-instance deployments targeting SQL Server.
        /// </summary>
        public EfCoreSagaBuilder<TSaga, TState, TDbContext> UseSqlServer()
        {
            builder.Services.Replace(ServiceDescriptor.Singleton<
                ISagaLockProvider<TDbContext>,
                SqlServerSagaLockProvider<TDbContext>>());
            return builder;
        }
    }
}
