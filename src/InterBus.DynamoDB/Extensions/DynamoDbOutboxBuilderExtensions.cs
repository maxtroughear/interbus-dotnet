using InterBus.DynamoDB.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using DynamoDbOutboxProcessor = InterBus.DynamoDB.Outbox.DynamoDbOutboxProcessor;

namespace InterBus.DynamoDB.Extensions;

/// <summary>Extension methods for configuring the InterBus DynamoDB transactional outbox.</summary>
public static class DynamoDbOutboxBuilderExtensions
{
    extension(InterBusBuilder builder)
    {
        /// <summary>
        /// Registers the DynamoDB outbox infrastructure: the scoped
        /// <see cref="IDynamoDbOutboxContext"/>, shared <see cref="DynamoDbOutboxOptions"/>,
        /// <see cref="DynamoDbOutboxConsumeState"/>, and the background
        /// <see cref="DynamoDbOutboxProcessor"/>. Does <b>not</b> add the bus-level publish
        /// middleware — call <see cref="UseDynamoDbOutbox(InterBusBuilder, Action{DynamoDbOutboxOptions}?)"/>
        /// to also route <see cref="IPublisher"/> calls through the outbox.
        /// <para>
        /// Call this method when you only want consumers (not the global bus publisher) to write
        /// to the outbox. Consumers opt in individually via
        /// <c>bus.AddConsumer&lt;T&gt;(c =&gt; c.UseDynamoDbOutbox())</c>.
        /// </para>
        /// </summary>
        /// <param name="configure">Optional delegate to configure <see cref="DynamoDbOutboxOptions"/>.</param>
        public InterBusBuilder AddDynamoDbOutbox(Action<DynamoDbOutboxOptions>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(builder);

            var options = new DynamoDbOutboxOptions();
            configure?.Invoke(options);

            builder.Services.AddSingleton(options);

            // Register the concrete context as scoped and expose it via the public interface.
            builder.Services.TryAddScoped<DynamoDbOutboxContext>();
            builder.Services.TryAddScoped<IDynamoDbOutboxContext>(sp => sp.GetRequiredService<DynamoDbOutboxContext>());

            // OutboxConsumeState bridges the publish middleware and the inline-delivery consume
            // middleware within a single dispatch scope.
            builder.Services.TryAddScoped<DynamoDbOutboxConsumeState>();

            if (options.RunOutboxProcessor)
                builder.Services.AddHostedService<DynamoDbOutboxProcessor>();

            if (options.CreateTableOnStartup)
                builder.Services.AddHostedService<DynamoDbOutboxTableInitializer>();

            return builder;
        }

        /// <summary>
        /// Enables the DynamoDB transactional outbox for InterBus.
        /// <para>
        /// When enabled, every <see cref="IPublisher"/> call buffers the message in the current
        /// scope's <see cref="IDynamoDbOutboxContext"/> instead of forwarding it to the transport
        /// directly. The message — together with any application-level
        /// <see cref="Amazon.DynamoDBv2.Model.TransactWriteItem"/>s registered on the same
        /// context — is committed atomically via a single DynamoDB
        /// <c>TransactWriteItems</c> request when
        /// <see cref="IDynamoDbOutboxContext.CommitAsync"/> is called.
        /// </para>
        /// <para>
        /// A background <see cref="Microsoft.Extensions.Hosting.IHostedService"/> continuously
        /// polls the outbox table and forwards pending groups to the configured transport. The
        /// poll delay (<see cref="DynamoDbOutboxOptions.PollInterval"/>) is only applied when the
        /// outbox is empty.
        /// </para>
        /// <para>
        /// The DynamoDB table specified by <see cref="DynamoDbOutboxOptions.TableName"/> must be
        /// created before use with partition key <c>GroupId</c> (String) and sort key
        /// <c>EntryId</c> (String).
        /// </para>
        /// </summary>
        /// <param name="configure">Optional delegate to configure <see cref="DynamoDbOutboxOptions"/>.</param>
        public InterBusBuilder UseDynamoDbOutbox(Action<DynamoDbOutboxOptions>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.AddDynamoDbOutbox(configure);
            builder.UsePublishMiddleware<DynamoDbOutboxPublishMiddleware>();

            return builder;
        }
    }
}
