using InterBus.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>
/// Extension methods for opting individual consumers into the transactional outbox.
/// </summary>
public static class ConsumerBuilderOutboxExtensions
{
    /// <summary>
    /// Routes messages published by this consumer (from within its <c>Consume</c> handler)
    /// through the transactional outbox backed by <typeparamref name="TDbContext"/>.
    /// <para>
    /// The consumer's publish calls are captured by
    /// <see cref="OutboxSendMiddleware{TDbContext}"/> and written to the outbox atomically with
    /// any other database writes the consumer makes before calling
    /// <c>SaveChangesAsync()</c>. After <c>Consume</c> returns,
    /// <see cref="OutboxConsumeMiddleware{TDbContext}"/> attempts to deliver the messages
    /// directly to the transport ("inline delivery"). If delivery succeeds the outbox group is
    /// deleted, saving a background-processor round-trip. If delivery fails the group remains in
    /// the outbox so the <see cref="OutboxProcessor{TDbContext}"/> background service can retry
    /// it after <see cref="OutboxOptions.ConsumerVisibilityTimeout"/> has elapsed.
    /// </para>
    /// <para>
    /// Requires <c>bus.UseOutbox&lt;TDbContext&gt;()</c> to be called on the InterBus builder so
    /// that <typeparamref name="TDbContext"/> is configured as the outbox store and the
    /// <see cref="OutboxOptions"/> singleton is registered.
    /// </para>
    /// </summary>
    /// <typeparam name="TDbContext">
    /// The application's <see cref="DbContext"/>. Must implement <see cref="IOutboxDbContext"/>.
    /// </typeparam>
    public static IConsumerBuilder UseOutbox<TDbContext>(this IConsumerBuilder builder)
        where TDbContext : DbContext, IOutboxDbContext
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Shared scoped state that lets OutboxSendMiddleware hand the written GroupId to
        // OutboxConsumeMiddleware so it can attempt inline delivery after Consume returns.
        builder.Services.TryAddScoped<OutboxConsumeState>();

        builder.UsePublishMiddleware(typeof(OutboxSendMiddleware<TDbContext>));
        builder.UseConsumeMiddleware(typeof(OutboxConsumeMiddleware<TDbContext>));

        return builder;
    }
}
