using InterBus.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>
/// Extension methods for opting individual consumers into the transactional outbox.
/// </summary>
public static class EfCoreOutboxConsumerBuilderExtensions
{
    extension(IConsumerBuilder builder)
    {
        /// <summary>
        /// Routes messages published by this consumer (from within its <c>Consume</c> handler)
        /// through the transactional outbox backed by <typeparamref name="TDbContext"/>.
        /// <para>
        /// The consumer's publish calls are captured by
        /// <see cref="OutboxPublishMiddleware{TDbContext}"/> and written to the outbox atomically with
        /// any other database writes the consumer makes before calling <c>SaveChangesAsync()</c>.
        /// The <see cref="OutboxProcessor{TDbContext}"/> background service forwards the messages
        /// to the transport once <see cref="OutboxGroup.VisibleAt"/> has elapsed.
        /// </para>
        /// <para>
        /// To also attempt immediate ("inline") delivery after <c>Consume</c> returns, call
        /// <see cref="UseOutboxWithInlineDelivery{TDbContext}"/> instead.
        /// </para>
        /// <para>
        /// Requires <c>bus.AddOutbox&lt;TDbContext&gt;()</c> or <c>bus.UseOutbox&lt;TDbContext&gt;()</c>
        /// to be called on the InterBus builder so that the outbox infrastructure and
        /// <see cref="OutboxOptions"/> singleton are registered.
        /// </para>
        /// </summary>
        /// <typeparam name="TDbContext">
        /// The application's <see cref="DbContext"/>
        /// </typeparam>
        public IConsumerBuilder UseOutbox<TDbContext>()
            where TDbContext : DbContext
        {
            ArgumentNullException.ThrowIfNull(builder);

            // OutboxConsumeState is registered globally by AddOutbox/UseOutbox, but ensure it's
            // available even when only AddOutbox is not called (defensive registration).
            builder.Services.TryAddScoped<OutboxConsumeState>();

            builder.UsePublishMiddleware<OutboxPublishMiddleware<TDbContext>>();

            return builder;
        }

        /// <summary>
        /// Routes messages published by this consumer through the transactional outbox and also
        /// attempts immediate ("inline") delivery after the consumer's <c>Consume</c> handler returns.
        /// <para>
        /// The consumer's publish calls are written to the outbox atomically with
        /// <c>SaveChangesAsync()</c>. After <c>Consume</c> returns,
        /// <see cref="OutboxConsumeMiddleware{TDbContext}"/> reads the committed group and forwards
        /// each message directly to the transport via <see cref="InterBus.Middleware.SendPipelineExecutor"/>,
        /// exactly as the background processor would. If delivery succeeds the group is deleted,
        /// saving a background-processor round-trip. If delivery fails the group remains in the
        /// outbox so the <see cref="OutboxProcessor{TDbContext}"/> background service can retry
        /// it after <see cref="OutboxGroup.VisibleAt"/> (controlled by
        /// <see cref="OutboxOptions.ConsumerVisibilityTimeout"/>) has elapsed.
        /// </para>
        /// <para>
        /// Requires <c>bus.AddOutbox&lt;TDbContext&gt;()</c> or <c>bus.UseOutbox&lt;TDbContext&gt;()</c>
        /// to be called on the InterBus builder so that the outbox infrastructure and
        /// <see cref="OutboxOptions"/> singleton are registered.
        /// </para>
        /// </summary>
        /// <typeparam name="TDbContext">
        /// The application's <see cref="DbContext"/>
        /// </typeparam>
        public IConsumerBuilder UseOutboxWithInlineDelivery<TDbContext>()
            where TDbContext : DbContext
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Services.TryAddScoped<OutboxConsumeState>();

            builder.UsePublishMiddleware<OutboxPublishMiddleware<TDbContext>>();
            builder.UseConsumeMiddleware<OutboxConsumeMiddleware<TDbContext>>();

            return builder;
        }
    }
}
