using Microsoft.Extensions.DependencyInjection;

namespace InterBus.RabbitMQ.Extensions;

/// <summary>Extension methods for configuring the RabbitMQ (AMQP) transport on an <see cref="InterBusBuilder"/>.</summary>
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
    /// <para>
    /// Connections are created with <c>AutomaticRecoveryEnabled = true</c>. The transport
    /// maintains one connection for publishing and the message pump maintains a separate
    /// connection for consuming.
    /// </para>
    /// <para>
    /// Separate scoped transports are registered for topic and queue destinations; each scope
    /// gets its own publish channel with publisher confirms enabled.
    /// </para>
    /// <para>
    /// Delayed delivery is not natively supported; the delay value is ignored and messages
    /// are delivered immediately.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Delegate to configure <see cref="RabbitMqTransportOptions"/>. At minimum set
    /// <see cref="RabbitMqTransportOptions.HostName"/> (or <see cref="RabbitMqTransportOptions.Uri"/>).
    /// </param>
    public static InterBusBuilder AddRabbitMqTransport(
        this InterBusBuilder builder,
        Action<RabbitMqTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new RabbitMqTransportOptions();
        configure?.Invoke(options);

        builder.Options.SetTransportUriSchemes(RabbitMqTopicTransport.TopicScheme, RabbitMqQueueTransport.QueueScheme);
        builder.Services.AddSingleton(options);

        // Connection provider manages publish and consume connections (singleton).
        builder.Services.AddSingleton<RabbitMqConnectionProvider>();
        builder.Services.AddSingleton<ITopologyBuilder, RabbitMqTopologyBuilder>();

        // Keyed registrations for transport factory scheme-based resolution.
        builder.Services.AddKeyedScoped<ITransport, RabbitMqTopicTransport>(RabbitMqTopicTransport.TopicScheme);
        builder.Services.AddKeyedScoped<ITransport, RabbitMqQueueTransport>(RabbitMqQueueTransport.QueueScheme);

        // Message pump runs as a background service.
        builder.Services.AddHostedService<RabbitMqMessagePump>();

        return builder;
    }
}
