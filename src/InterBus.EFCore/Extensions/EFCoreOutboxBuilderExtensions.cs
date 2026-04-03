using InterBus.EFCore.Internal;
using InterBus.Middleware;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.EFCore.Extensions;

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
    /// A background <see cref="Microsoft.Extensions.Hosting.IHostedService"/> polls the
    /// outbox table every <see cref="OutboxOptions.PollInterval"/> and forwards pending
    /// messages to the configured transport.
    /// </para>
    /// <para>
    /// <typeparamref name="TDbContext"/> must implement <see cref="IOutboxDbContext"/> and
    /// call <c>modelBuilder.AddInterBusOutbox()</c> inside <c>OnModelCreating</c>.
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
