using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Fluent builder for configuring the InterBus transactional outbox.
/// Obtained from <c>builder.UseOutbox&lt;TDbContext&gt;(outbox => { ... })</c>.
/// <para>
/// Use the extension methods on this type to select a database-specific locking strategy
/// and configure polling behaviour:
/// <code>
/// bus.UseOutbox&lt;AppDbContext&gt;(outbox => outbox
///     .WithPollInterval(TimeSpan.FromSeconds(2))
///     .UsePostgreSql());
/// </code>
/// </para>
/// </summary>
/// <typeparam name="TDbContext">
/// The application's <see cref="DbContext"/>
/// </typeparam>
public sealed class OutboxBuilder<TDbContext>
    where TDbContext : DbContext
{
    /// <summary>The underlying service collection, used by lock-provider extension methods.</summary>
    private IServiceCollection Services { get; }

    internal OutboxOptions Options { get; } = new();

    internal OutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Overrides how often the background processor polls the outbox table when there are no
    /// pending messages. Defaults to 5 seconds. Lower values reduce end-to-end latency at the
    /// cost of more frequent database queries when the outbox is empty.
    /// </summary>
    public OutboxBuilder<TDbContext> WithPollInterval(TimeSpan interval)
    {
        Options.PollInterval = interval;
        return this;
    }

    public OutboxBuilder<TDbContext> DisableOutboxProcessor()
    {
        Options.RunOutboxProcessor = false;
        return this;
    }
    
    /// <summary>
    /// Configures the outbox to use the default EF Core lock provider.
    /// Uses plain LINQ queries with no database-level row locking — safe for single-instance
    /// deployments and integration tests. This is the default and does not need to be called
    /// explicitly.
    /// </summary>
    public OutboxBuilder<TDbContext> UseDefaultLockProvider()
    {
        Services.Replace(ServiceDescriptor.Singleton<
            IOutboxLockProvider<TDbContext>,
            EfCoreOutboxLockProvider<TDbContext>>());
        return this;
    }

    /// <summary>
    /// Configures the outbox to use the PostgreSQL lock provider.
    /// Uses a CTE with <c>FOR UPDATE SKIP LOCKED</c> to atomically fetch and lock the next
    /// pending group. Recommended for multi-instance deployments targeting PostgreSQL.
    /// </summary>
    public OutboxBuilder<TDbContext> UsePostgreSql()
    {
        Services.Replace(ServiceDescriptor.Singleton<
            IOutboxLockProvider<TDbContext>,
            PostgreSqlOutboxLockProvider<TDbContext>>());
        return this;
    }

    /// <summary>
    /// Configures the outbox to use the SQL Server lock provider.
    /// Uses a CTE with <c>UPDLOCK, READPAST</c> hints to atomically fetch and lock the next
    /// pending group. Recommended for multi-instance deployments targeting SQL Server.
    /// </summary>
    public OutboxBuilder<TDbContext> UseSqlServer()
    {
        Services.Replace(ServiceDescriptor.Singleton<
            IOutboxLockProvider<TDbContext>,
            SqlServerOutboxLockProvider<TDbContext>>());
        return this;
    }
}
