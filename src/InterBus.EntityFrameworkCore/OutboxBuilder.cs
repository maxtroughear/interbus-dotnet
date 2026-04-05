using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.EntityFrameworkCore;

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
/// The application's <see cref="DbContext"/>. Must implement <see cref="IOutboxDbContext"/>.
/// </typeparam>
public sealed class OutboxBuilder<TDbContext>
    where TDbContext : DbContext, IOutboxDbContext
{
    /// <summary>The underlying service collection, used by lock-provider extension methods.</summary>
    public IServiceCollection Services { get; }

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
}

