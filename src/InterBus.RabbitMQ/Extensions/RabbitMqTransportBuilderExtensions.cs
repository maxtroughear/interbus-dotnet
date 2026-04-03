using Microsoft.Extensions.DependencyInjection;

namespace InterBus.RabbitMQ.Extensions;

/// <summary>Extension methods for configuring the RabbitMQ (AMQP) transport on an <see cref="IInterBusBuilder"/>.</summary>
public static class RabbitMqTransportBuilderExtensions
{
    /// <summary>
    /// Use a RabbitMQ (AMQP) backed transport for message publishing and consumption.
    /// <para>
    /// Topic (fan-out) messages are published to a <c>fanout</c> exchange named
    /// <c>{ExchangePrefix}{topicName}</c>. All queues bound to the exchange receive a copy.
    /// </para>
    /// <para>
    /// Queue (point-to-point) messages are published to the default exchange with the queue
    /// name as the routing key, delivering the message to exactly one consumer.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Delegate to configure <see cref="RabbitMqTransportOptions"/>. At minimum set
    /// <see cref="RabbitMqTransportOptions.HostName"/> (or <see cref="RabbitMqTransportOptions.Uri"/>).
    /// </param>
    public static IInterBusBuilder AddRabbitMqTransport(
        this IInterBusBuilder builder,
        Action<RabbitMqTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new RabbitMqTransportOptions();
        configure?.Invoke(options);

        builder.Options.SetTransportUriSchemes(RabbitMqTransport.TopicScheme, RabbitMqTransport.QueueScheme);
        builder.Services.AddSingleton(options);

        // Register the concrete type so keyed registrations can share the same singleton.
        builder.Services.AddSingleton<RabbitMqTransport>();

        // Keyed registrations for transport factory scheme-based resolution.
        builder.Services.AddKeyedSingleton<IInterBusTransport>(RabbitMqTransport.TopicScheme,
            (sp, _key) => sp.GetRequiredService<RabbitMqTransport>());
        builder.Services.AddKeyedSingleton<IInterBusTransport>(RabbitMqTransport.QueueScheme,
            (sp, _key) => sp.GetRequiredService<RabbitMqTransport>());

        // Keep the non-keyed registration for backward compatibility.
        builder.Services.AddSingleton<IInterBusTransport>(sp => sp.GetRequiredService<RabbitMqTransport>());

        // Message pump runs as a background service.
        builder.Services.AddHostedService<RabbitMqMessagePump>();

        return builder;
    }
}
