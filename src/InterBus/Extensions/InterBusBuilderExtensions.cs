using System.Text.Json;
using InterBus.Internal;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Extensions;

/// <summary>Extension methods for <see cref="IInterBusBuilder"/>.</summary>
public static class InterBusBuilderExtensions
{
    /// <summary>
    /// Register the InterBus scheduling infrastructure. This enables injecting
    /// <see cref="IMessageScheduler"/> to schedule messages for deferred delivery.
    /// <para>
    /// A concrete <see cref="IMessageSchedulingProvider"/> must also be registered (e.g. via
    /// <c>AddTickerQScheduler()</c>) to supply the underlying delivery mechanism.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    public static IInterBusBuilder AddScheduling(this IInterBusBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddScoped<IMessageScheduler, MessageScheduler>();
        return builder;
    }

    /// <summary>
    /// Register a publish-based <see cref="IMessageScheduler"/> that schedules messages by
    /// publishing a concrete command message to the bus. The command is consumed by a
    /// <see cref="SchedulingConsumer"/> (registered via <see cref="AddSchedulingConsumer"/>),
    /// which enlists the registered <see cref="IMessageSchedulingProvider"/> to perform the
    /// actual deferred delivery.
    /// <para>
    /// Use <see cref="AddSchedulingConsumer"/> in the same application or in a dedicated
    /// consumer service to process the scheduling commands.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    public static IInterBusBuilder AddPublishScheduling(this IInterBusBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddScoped<IMessageScheduler, PublishMessageScheduler>();
        return builder;
    }

    /// <summary>
    /// Register the <see cref="SchedulingConsumer"/> that processes scheduling command messages
    /// published by <see cref="AddPublishScheduling"/> and enlists the registered
    /// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
    /// <para>
    /// Requires an <see cref="IMessageSchedulingProvider"/> to be registered in DI
    /// (e.g. via <c>AddTickerQScheduler()</c>).
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    public static IInterBusBuilder AddSchedulingConsumer(this IInterBusBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var deliveryCommandTypeName = typeof(ScheduleDeliveryCommand).FullName
                                      ?? nameof(ScheduleDeliveryCommand);

        builder.AddConsumer<SchedulingConsumer>(
            [typeof(ScheduleDeliveryCommand)],
            (ctx, sp, ct) =>
            {
                var consumer = sp.GetRequiredService<SchedulingConsumer>();
                var serializer = sp.GetRequiredService<IMessageSerializer>();
                var consumePipeline = sp.GetRequiredService<ConsumePipelineExecutor>();
                var messageType = ctx.Headers[InterBusHeaders.MessageType];

                if (messageType == deliveryCommandTypeName)
                {
                    var cmd = serializer.Deserialize<ScheduleDeliveryCommand>(ctx.Payload)!;
                    var consumeContext = new ConsumeContext<ScheduleDeliveryCommand>(ctx.Descriptor, cmd, ctx.Headers);
                    return consumePipeline.ExecuteAsync(consumeContext, consumer.Consume, ct);
                }

                return Task.CompletedTask;
            });

        return builder;
    }

    /// <summary>
    /// Register the transport-delay scheduling provider. Scheduled messages are delivered
    /// immediately via the configured transport with a <see cref="TransportSendContext.Delay"/>
    /// equal to the requested deferral duration. Transports that support deferred visibility
    /// (e.g. PostgreSQL <c>visible_at</c>) will honour the delay; transports that do not will
    /// deliver the message immediately.
    /// <para>
    /// This provider requires no additional dependencies beyond the configured transport and
    /// is suitable for environments where a dedicated scheduler (e.g. TickerQ) is not available.
    /// </para>
    /// <para>
    /// Call <c>bus.AddScheduling()</c> before calling this method to register the
    /// <see cref="IMessageScheduler"/> infrastructure.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    public static IInterBusBuilder AddTransportDelayScheduler(this IInterBusBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddScoped<IMessageSchedulingProvider, TransportDelaySchedulingProvider>();
        return builder;
    }

    /// <summary>
    /// Configures the number of concurrent message handlers for the specified consumer.
    /// Transports that support concurrent dispatch per queue (e.g. RabbitMQ) read this value
    /// from <see cref="ConsumerDescriptor.ConcurrencyCount"/> when setting up their consumer channels.
    /// Defaults to <c>1</c> (sequential dispatch) when not configured.
    /// <para>
    /// Must be called after the consumer has been registered (e.g. after <c>AddConsumers()</c>).
    /// </para>
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to configure.</typeparam>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="concurrencyCount">
    /// The number of concurrent message handlers. Must be at least <c>1</c>.
    /// </param>
    public static IInterBusBuilder WithConsumerConcurrency<TConsumer>(
        this IInterBusBuilder builder,
        int concurrencyCount)
        where TConsumer : class, IConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (concurrencyCount < 1)
            throw new ArgumentOutOfRangeException(nameof(concurrencyCount), "Concurrency count must be at least 1.");

        var descriptor = builder.Options.Consumers.FirstOrDefault(d => d.ConsumerType == typeof(TConsumer));
        if (descriptor is null)
            throw new InvalidOperationException(
                $"Consumer '{typeof(TConsumer).Name}' has not been registered. " +
                $"Call AddConsumer<{typeof(TConsumer).Name}>() (or AddConsumers()) before calling WithConsumerConcurrency.");

        descriptor.ConcurrencyCount = concurrencyCount;

        return builder;
    }

    /// <summary>
    /// Register a <see cref="SystemTextJsonMessageSerializer"/> configured with the provided
    /// <paramref name="options"/>, replacing the default reflection-based serialiser.
    /// <para>
    /// Designed for trimmed or Native AOT applications where a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> must be supplied:
    /// </para>
    /// <code>
    /// bus.UseSystemTextJsonSerializer(MyAotJsonContext.Default.Options);
    /// </code>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="options">
    /// <see cref="JsonSerializerOptions"/> to pass to <see cref="SystemTextJsonMessageSerializer"/>.
    /// Typically <c>MyJsonContext.Default.Options</c> from a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>.
    /// </param>
    public static IInterBusBuilder UseSystemTextJsonSerializer(
        this IInterBusBuilder builder,
        JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        builder.Services.Replace(
            ServiceDescriptor.Singleton<IMessageSerializer>(
                new SystemTextJsonMessageSerializer(options)));
        return builder;
    }
}
