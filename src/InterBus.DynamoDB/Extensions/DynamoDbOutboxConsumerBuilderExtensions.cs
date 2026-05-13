using InterBus.Consuming;
using InterBus.DynamoDB.Outbox;
using InterBus.DynamoDB.Outbox.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.DynamoDB.Extensions;

/// <summary>
/// Extension methods for opting individual consumers into the DynamoDB transactional outbox.
/// </summary>
public static class DynamoDbOutboxConsumerBuilderExtensions
{
    extension(ConsumerBuilder builder)
    {
        /// <summary>
        /// Routes messages published by this consumer (from within its <c>Consume</c> handler)
        /// through the DynamoDB transactional outbox.
        /// <para>
        /// The consumer's publish calls are captured by
        /// <see cref="DynamoDbOutboxPublishMiddleware"/> and will be forwarded to the transport
        /// only after the consumer calls <see cref="IDynamoDbOutboxContext.CommitAsync"/>.
        /// The background <see cref="DynamoDbOutboxProcessor"/> will process committed groups.
        /// </para>
        /// <para>
        /// To also attempt immediate ("inline") delivery after <c>Consume</c> returns, use
        /// <see cref="UseDynamoDbOutboxWithInlineDelivery(ConsumerBuilder)"/> instead.
        /// </para>
        /// <para>
        /// Requires <c>bus.AddDynamoDbOutbox()</c> or <c>bus.UseDynamoDbOutbox()</c> to have
        /// been called on the InterBus builder so that the outbox infrastructure is registered.
        /// </para>
        /// </summary>
        public ConsumerBuilder UseDynamoDbOutbox()
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Services.TryAddScoped<DynamoDbOutboxConsumeState>();
            builder.Services.TryAddScoped<DynamoDbOutboxContext>();
            builder.Services.TryAddScoped<IDynamoDbOutboxContext>(sp => sp.GetRequiredService<DynamoDbOutboxContext>());

            builder.UsePublishMiddleware<DynamoDbOutboxPublishMiddleware>();

            return builder;
        }

        /// <summary>
        /// Routes messages published by this consumer through the DynamoDB transactional outbox
        /// and also attempts immediate ("inline") delivery after the consumer's <c>Consume</c>
        /// handler returns.
        /// <para>
        /// After <c>Consume</c> returns (including the
        /// <see cref="IDynamoDbOutboxContext.CommitAsync"/> call that persists the outbox group),
        /// <see cref="DynamoDbOutboxInlineDeliveryConsumeMiddleware"/> reads the committed group
        /// and forwards each message directly to the transport, exactly as the background
        /// processor would. If delivery succeeds the group is deleted, saving a background
        /// polling round-trip. If delivery fails the group remains in the outbox so the
        /// <see cref="DynamoDbOutboxProcessor"/> can retry it after
        /// <see cref="DynamoDbOutboxOptions.ConsumerVisibilityTimeout"/> has elapsed.
        /// </para>
        /// <para>
        /// Requires <c>bus.AddDynamoDbOutbox()</c> or <c>bus.UseDynamoDbOutbox()</c> to have
        /// been called on the InterBus builder.
        /// </para>
        /// </summary>
        public ConsumerBuilder UseDynamoDbOutboxWithInlineDelivery()
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Services.TryAddScoped<DynamoDbOutboxConsumeState>();
            builder.Services.TryAddScoped<DynamoDbOutboxContext>();
            builder.Services.TryAddScoped<IDynamoDbOutboxContext>(sp => sp.GetRequiredService<DynamoDbOutboxContext>());

            builder.UsePublishMiddleware<DynamoDbOutboxPublishMiddleware>();
            builder.UseConsumeMiddleware<DynamoDbOutboxInlineDeliveryConsumeMiddleware>();

            return builder;
        }
    }
}
