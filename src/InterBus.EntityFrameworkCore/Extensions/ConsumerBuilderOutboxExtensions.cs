using InterBus.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore;

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
    /// The middleware first attempts to deliver each message directly to the transport. If
    /// delivery succeeds, no outbox entry is written. If delivery fails, the message is written
    /// to the outbox so the <see cref="OutboxProcessor{TDbContext}"/> background service can
    /// retry it. When <see cref="OutboxOptions.ConsumerVisibilityTimeout"/> is configured, the
    /// outbox group is stamped with a <c>VisibleAt</c> timestamp to prevent the processor from
    /// picking up the entry before the consumer's surrounding transaction has committed.
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
        return builder.UsePublishMiddleware(typeof(OutboxSendMiddleware<TDbContext>));
    }
}
