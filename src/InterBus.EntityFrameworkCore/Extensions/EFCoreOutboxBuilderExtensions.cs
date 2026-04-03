using InterBus.EntityFrameworkCore.Internal;
using InterBus.Middleware;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the InterBus transactional outbox.</summary>
public static class EFCoreOutboxBuilderExtensions
{
    /// <summary>
    /// Enables the transactional outbox pattern for InterBus using an existing EF Core
    /// <typeparamref name="TDbContext"/> as the outbox store.
    /// <para>
    /// When enabled, every <see cref="InterBus.IMessagePublisher"/> call writes the message
    /// to the outbox table in the caller's <typeparamref name="TDbContext"/> instead of
    /// forwarding it to the transport directly. The message is only sent once the caller
    /// commits its database transaction (i.e. calls
    /// <c>dbContext.SaveChangesAsync()</c>), guaranteeing atomicity with any surrounding
    /// business-logic writes.
    /// </para>
    /// <para>
    /// A background <see cref="Microsoft.Extensions.Hosting.IHostedService"/> continuously
    /// polls the outbox table and forwards pending groups to the configured transport. The
    /// poll delay (<see cref="OutboxOptions.PollInterval"/>) is only applied when the outbox
    /// is empty.
    /// </para>
    /// <para>
    /// <typeparamref name="TDbContext"/> must implement <see cref="IOutboxDbContext"/> and
    /// call <c>modelBuilder.AddInterBusOutbox()</c> inside <c>OnModelCreating</c>.
    /// </para>
    /// <para>
    /// Configure <see cref="OutboxOptions.LockProvider"/> to use a database-specific
    /// locking strategy for multi-instance deployments:
    /// <code>
    /// bus.UseOutbox&lt;AppDbContext&gt;(o => o.LockProvider = new PostgreSqlOutboxLockProvider());
    /// </code>
    /// </para>
    /// </summary>
    /// <typeparam name="TDbContext">
    /// The application's <see cref="DbContext"/>. Must implement <see cref="IOutboxDbContext"/>.
    /// </typeparam>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">Optional delegate to configure <see cref="OutboxOptions"/>.</param>
    public static IInterBusBuilder UseOutbox<TDbContext>(
        this IInterBusBuilder builder,
        Action<OutboxOptions>? configure = null)
        where TDbContext : DbContext, IOutboxDbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new OutboxOptions();
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddScoped<IPublishMiddleware, OutboxPublishMiddleware<TDbContext>>();
        builder.Services.AddHostedService<OutboxProcessor<TDbContext>>();

        return builder;
    }
}
