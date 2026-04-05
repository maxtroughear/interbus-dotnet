using InterBus.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for selecting a lock provider on <see cref="OutboxBuilder{TDbContext}"/>.</summary>
public static class OutboxBuilderExtensions
{
    extension<TDbContext>(OutboxBuilder<TDbContext> builder) where TDbContext : DbContext, IOutboxDbContext
    {
        /// <summary>
        /// Configures the outbox to use the default EF Core lock provider.
        /// Uses plain LINQ queries with no database-level row locking — safe for single-instance
        /// deployments and integration tests. This is the default and does not need to be called
        /// explicitly.
        /// </summary>
        public OutboxBuilder<TDbContext> UseDefaultLockProvider()
        {
            builder.Services.Replace(ServiceDescriptor.Singleton<
                IOutboxLockProvider<TDbContext>,
                EfCoreOutboxLockProvider<TDbContext>>());
            return builder;
        }

        /// <summary>
        /// Configures the outbox to use the PostgreSQL lock provider.
        /// Uses a CTE with <c>FOR UPDATE SKIP LOCKED</c> to atomically fetch and lock the next
        /// pending group. Recommended for multi-instance deployments targeting PostgreSQL.
        /// </summary>
        public OutboxBuilder<TDbContext> UsePostgreSql()
        {
            builder.Services.Replace(ServiceDescriptor.Singleton<
                IOutboxLockProvider<TDbContext>,
                PostgreSqlOutboxLockProvider<TDbContext>>());
            return builder;
        }

        /// <summary>
        /// Configures the outbox to use the SQL Server lock provider.
        /// Uses a CTE with <c>UPDLOCK, READPAST</c> hints to atomically fetch and lock the next
        /// pending group. Recommended for multi-instance deployments targeting SQL Server.
        /// </summary>
        public OutboxBuilder<TDbContext> UseSqlServer()
        {
            builder.Services.Replace(ServiceDescriptor.Singleton<
                IOutboxLockProvider<TDbContext>,
                SqlServerOutboxLockProvider<TDbContext>>());
            return builder;
        }
    }
}

