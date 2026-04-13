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
/// The application's <see cref="DbContext"/>
/// </typeparam>
public sealed class OutboxBuilder<TDbContext>
    where TDbContext : DbContext
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

    /// <summary>
    /// Sets the visibility timeout for outbox groups created by consumer publish calls.
    /// When a consumer publishes a message during its consume handler, the outbox group is
    /// created with a <c>VisibleAt</c> timestamp of <c>UtcNow + timeout</c>, preventing the
    /// outbox processor from picking it up until that time has elapsed.
    /// <para>
    /// Defaults to 5 seconds as a safe value when inline delivery is configured (via
    /// <c>UseOutboxWithInlineDelivery&lt;TDbContext&gt;()</c>) to minimise the risk of the
    /// background processor racing with inline delivery. Set to
    /// <see cref="TimeSpan.Zero"/> to make consumer-published groups immediately eligible.
    /// </para>
    /// </summary>
    public OutboxBuilder<TDbContext> WithConsumerVisibilityTimeout(TimeSpan timeout)
    {
        Options.ConsumerVisibilityTimeout = timeout;
        return this;
    }

    public OutboxBuilder<TDbContext> DisableOutboxProcessor()
    {
        Options.RunOutboxProcessor = false;
        return this;
    }
}
