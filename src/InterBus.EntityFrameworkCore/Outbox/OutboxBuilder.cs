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
///     .WithEmptyPollDelay(TimeSpan.FromSeconds(2))
///     .WithMaxConcurrentWorkers(8)
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
    /// Overrides how long outbox workers wait before polling again when no claimable groups are
    /// found. Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of more
    /// frequent database queries when the outbox is empty.
    /// </summary>
    public OutboxBuilder<TDbContext> WithEmptyPollDelay(TimeSpan delay)
    {
        Options.EmptyPollDelay = delay;
        return this;
    }

    /// <summary>
    /// Sets the number of independent FIFO processing shards. Defaults to <c>128</c>.
    /// <para>
    /// With a single shard all outbox groups are delivered in strict global FIFO order.
    /// Increasing the shard count allows multiple processor instances to run in parallel
    /// (higher throughput) at the expense of global ordering — ordering is only guaranteed
    /// within each shard.
    /// </para>
    /// <para>
    /// Effective only when combined with a database-level lock provider
    /// (<c>UsePostgreSql()</c> or <c>UseSqlServer()</c>).
    /// </para>
    /// </summary>
    public OutboxBuilder<TDbContext> WithShardCount(int shardCount)
    {
        if (shardCount < 1)
            throw new ArgumentOutOfRangeException(nameof(shardCount), "Shard count must be >= 1");

        Options.ShardCount = shardCount;
        return this;
    }

    /// <summary>
    /// Sets the number of concurrent outbox workers this process runs.
    /// Each worker claims and processes one group at a time in its own transaction.
    /// </summary>
    public OutboxBuilder<TDbContext> WithMaxConcurrentWorkers(int workerCount)
    {
        if (workerCount < 1)
            throw new ArgumentOutOfRangeException(nameof(workerCount), "Worker count must be >= 1");

        Options.MaxConcurrentWorkers = workerCount;
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
