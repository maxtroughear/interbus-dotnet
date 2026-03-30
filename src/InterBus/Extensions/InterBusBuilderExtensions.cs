using System.Text.Json;
using InterBus.Internal;
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

        var publishCommandTypeName = typeof(SchedulePublishCommand).FullName
                                     ?? typeof(SchedulePublishCommand).Name;
        var sendCommandTypeName = typeof(ScheduleSendCommand).FullName
                                  ?? typeof(ScheduleSendCommand).Name;

        builder.AddConsumer<SchedulingConsumer>(
            [typeof(SchedulePublishCommand), typeof(ScheduleSendCommand)],
            (sp, payload, ctx, ct) =>
            {
                var consumer = sp.GetRequiredService<SchedulingConsumer>();
                var serializer = sp.GetRequiredService<IMessageSerializer>();
                var messageType = ctx.MessageType;

                if (messageType == publishCommandTypeName)
                {
                    var cmd = serializer.Deserialize<SchedulePublishCommand>(payload)!;
                    return consumer.Consume(cmd, ctx, ct);
                }

                if (messageType == sendCommandTypeName)
                {
                    var cmd = serializer.Deserialize<ScheduleSendCommand>(payload)!;
                    return consumer.Consume(cmd, ctx, ct);
                }

                return Task.CompletedTask;
            });

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
