using InterBus.EntityFrameworkCore.LockProviders;
using InterBus.EntityFrameworkCore.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the InterBus transactional outbox.</summary>
public static class EfCoreOutboxBusBuilderExtensions
{
    /// <param name="builder">The InterBus builder.</param>
    extension(InterBusBuilder builder)
    {
        /// <summary>
        /// Registers the transactional outbox infrastructure for InterBus: the lock provider,
        /// shared options, <see cref="OutboxConsumeState"/>, and the background
        /// <see cref="OutboxProcessor{TDbContext}"/>. Does <b>not</b> add the bus-level publish
        /// middleware — call <see cref="EfCoreOutboxBusBuilderExtensions.UseEfCoreOutbox{TDbContext}"/>
        /// to also route <see cref="IPublisher"/> calls through the outbox.
        /// <para>
        /// Call this method when you only want consumers (not the global bus publisher) to write to
        /// the outbox. Consumers opt in individually via
        /// <c>bus.AddConsumer&lt;T&gt;(c =&gt; c.UseOutbox&lt;TDbContext&gt;())</c>.
        /// </para>
        /// <para>
        /// <typeparamref name="TDbContext"/> must call <c>modelBuilder.AddInterBusOutbox()</c>
        /// inside <c>OnModelCreating</c>.
        /// </para>
        /// </summary>
        /// <typeparam name="TDbContext">
        /// The application's <see cref="DbContext"/>
        /// </typeparam>
        public InterBusBuilder AddEfCore<TDbContext>(
            Action<EfCoreBuilder<TDbContext>>? configure = null)
            where TDbContext : DbContext
        {
            ArgumentNullException.ThrowIfNull(builder);

            // Register the default lock provider; lock-provider extension methods replace it.
            builder.Services.AddSingleton<IOutboxLockProvider<TDbContext>, EfCoreOutboxLockProvider<TDbContext>>();
            builder.Services.AddSingleton<ILockProvider, InMemoryLockProvider>();

            // Register default OutboxOptions so OutboxPublishMiddleware can always resolve it.
            // UseEfCoreOutbox replaces this with the user-configured instance.
            builder.Services.TryAddSingleton<OutboxOptions>();

            configure?.Invoke(new EfCoreBuilder<TDbContext>(builder.Services));

            return builder;
        }

        /// <summary>
        /// Enables the transactional outbox pattern for InterBus using an existing EF Core
        /// <typeparamref name="TDbContext"/> as the outbox store.
        /// <para>
        /// When enabled, every <see cref="IPublisher"/> call writes the message
        /// to the outbox table in the caller's <typeparamref name="TDbContext"/> instead of
        /// forwarding it to the transport directly. The message is only sent once the caller
        /// commits its database transaction (i.e. calls
        /// <c>dbContext.SaveChangesAsync()</c>), guaranteeing atomicity with any surrounding
        /// business-logic writes.
        /// </para>
        /// <para>
        /// A background <see cref="Microsoft.Extensions.Hosting.IHostedService"/> continuously
        /// polls the outbox table and forwards pending groups to the configured transport. The
        /// empty poll delay (<see cref="OutboxOptions.EmptyPollDelay"/>) is only applied when no
        /// claimable groups are available.
        /// </para>
        /// <para>
        /// <typeparamref name="TDbContext"/> must call <c>modelBuilder.AddInterBusOutbox()</c>
        /// inside <c>OnModelCreating</c>.
        /// </para>
        /// <para>
        /// Use the <see cref="OutboxBuilder{TDbContext}"/> delegate to configure options and select
        /// a database-specific locking strategy for multi-instance deployments:
        /// <code>
        /// bus.UseOutbox&lt;AppDbContext&gt;(outbox => outbox
        ///     .WithEmptyPollDelay(TimeSpan.FromSeconds(2))
        ///     .UsePostgreSql());
        /// </code>
        /// </para>
        /// <para>
        /// To route only consumer-published messages through the outbox (not the global bus
        /// publisher), use <see cref="EfCoreOutboxBusBuilderExtensions.AddEfCore{TDbContext}"/>
        /// instead and opt consumers in individually.
        /// </para>
        /// </summary>
        /// <typeparam name="TDbContext">
        /// The application's <see cref="DbContext"/>.
        /// </typeparam>
        /// <param name="configure">Optional delegate to configure the <see cref="OutboxBuilder{TDbContext}"/>.</param>
        public InterBusBuilder UseEfCoreOutbox<TDbContext>(
            Action<OutboxBuilder<TDbContext>>? configure = null)
            where TDbContext : DbContext
        {
            ArgumentNullException.ThrowIfNull(builder);

            var outboxBuilder = new OutboxBuilder<TDbContext>(builder.Services);
            configure?.Invoke(outboxBuilder);

            builder.Services.AddSingleton(outboxBuilder.Options);

            builder.Services.TryAddScoped<OutboxConsumeState>();
            builder.UsePublishMiddleware<OutboxPublishMiddleware<TDbContext>>();

            if (outboxBuilder.Options.RunOutboxProcessor)
                builder.Services.AddHostedService<OutboxProcessor<TDbContext>>();

            return builder;
        }
    }
}
