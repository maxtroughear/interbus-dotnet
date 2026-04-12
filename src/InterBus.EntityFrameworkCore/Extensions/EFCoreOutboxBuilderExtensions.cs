using InterBus.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the InterBus transactional outbox.</summary>
public static class EfCoreOutboxBuilderExtensions
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
    /// Use the <see cref="OutboxBuilder{TDbContext}"/> delegate to configure options and select
    /// a database-specific locking strategy for multi-instance deployments:
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
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">Optional delegate to configure the <see cref="OutboxBuilder{TDbContext}"/>.</param>
    public static IInterBusBuilder UseOutbox<TDbContext>(
        this IInterBusBuilder builder,
        Action<OutboxBuilder<TDbContext>>? configure = null)
        where TDbContext : DbContext, IOutboxDbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Register the default lock provider; lock-provider extension methods replace it.
        builder.Services.AddSingleton<IOutboxLockProvider<TDbContext>, EfCoreOutboxLockProvider<TDbContext>>();

        var outboxBuilder = new OutboxBuilder<TDbContext>(builder.Services);
        configure?.Invoke(outboxBuilder);

        builder.Services.AddSingleton(outboxBuilder.Options);
        // OutboxConsumeState bridges OutboxSendMiddleware and OutboxConsumeMiddleware within a
        // single dispatch scope. Register globally so that OutboxSendMiddleware can be resolved
        // in any scope (including the bus-context scope where no consume middleware is active).
        builder.Services.TryAddScoped<OutboxConsumeState>();
        builder.UsePublishMiddleware<OutboxSendMiddleware<TDbContext>>();

        if (outboxBuilder.Options.RunOutboxProcessor)
            builder.Services.AddHostedService<OutboxProcessor<TDbContext>>();

        return builder;
    }
}
