using InterBus.Consuming;
using InterBus.DynamoDB.Outbox;
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
        /// The background <see cref="Outbox.DynamoDbOutboxProcessor"/> will process committed groups.
        /// </para>
        /// <para>
        /// Requires <c>bus.AddDynamoDbOutbox()</c> or <c>bus.UseDynamoDbOutbox()</c> to have
        /// been called on the InterBus builder so that the outbox infrastructure is registered.
        /// </para>
        /// </summary>
        public ConsumerBuilder UseDynamoDbOutbox()
        {
            builder.Services.TryAddScoped<DynamoDbOutboxConsumeState>();
            builder.Services.TryAddScoped<DynamoDbOutboxContext>();
            builder.Services.TryAddScoped<IDynamoDbOutboxContext>(sp => sp.GetRequiredService<DynamoDbOutboxContext>());

            builder.UsePublishMiddleware<DynamoDbOutboxPublishMiddleware>();

            return builder;
        }

        /// <summary>
        /// Enables stateful DynamoDB inbox+outbox processing for this consumer.
        /// <para>
        /// The middleware persists per-consumer inbox state in
        /// <see cref="DynamoDbOutboxOptions.InboxTableName"/> so redeliveries can skip
        /// rerunning consumer code and retry only outbox delivery when needed.
        /// </para>
        /// <para>
        /// When the consumer publishes messages, they are committed transactionally with inbox
        /// state and any additional <see cref="IDynamoDbOutboxContext.AddTransactItem"/> writes.
        /// </para>
        /// <para>
        /// This flow is intended to run without the background outbox processor
        /// (<see cref="DynamoDbOutboxOptions.RunOutboxProcessor"/> set to <c>false</c>).
        /// </para>
        /// </summary>
        public ConsumerBuilder UseDynamoDbInboxOutbox()
        {
            builder.Services.TryAddScoped<DynamoDbOutboxConsumeState>();
            builder.Services.TryAddScoped<DynamoDbOutboxContext>();
            builder.Services.TryAddScoped<IDynamoDbOutboxContext>(sp => sp.GetRequiredService<DynamoDbOutboxContext>());

            builder.UseConsumeMiddleware<DynamoDbInboxOutboxConsumeMiddleware>();
            builder.UsePublishMiddleware<DynamoDbInboxOutboxPublishMiddleware>();

            return builder;
        }
    }
}
