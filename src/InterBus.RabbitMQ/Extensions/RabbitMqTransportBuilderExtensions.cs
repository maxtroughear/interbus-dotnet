using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
    /// <para>
    /// Connections are created with <c>AutomaticRecoveryEnabled = true</c>. The transport
    /// maintains one connection for publishing and the message pump maintains a separate
    /// connection for consuming.
    /// </para>
    /// <para>
    /// Publisher confirms are enabled on the publish channel so that every
    /// <see cref="IInterBusTransport.SendAsync"/> call waits for a broker acknowledgement
    /// before returning.
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
    public static IInterBusBuilder AddRabbitMqTransport(
        this IInterBusBuilder builder,
        Action<RabbitMqTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new RabbitMqTransportOptions();
        configure?.Invoke(options);

        builder.Options.SetTransportUriSchemes(RabbitMqTransport.TopicScheme, RabbitMqTransport.QueueScheme);
        builder.Services.AddSingleton(options);

        // Connection provider manages publish and consume connections.
        builder.Services.AddSingleton<RabbitMqConnectionProvider>();

        // Register the concrete type so keyed registrations can share the same singleton.
        // Use a factory so DI can construct via the internal RabbitMqConnectionProvider.
        builder.Services.AddSingleton<RabbitMqTransport>(sp => new RabbitMqTransport(
            sp.GetRequiredService<RabbitMqConnectionProvider>(),
            sp.GetRequiredService<RabbitMqTransportOptions>(),
            sp.GetRequiredService<ILogger<RabbitMqTransport>>()));

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

    /// <summary>
    /// Configures the number of concurrent message handlers for a specific consumer's queue.
    /// This maps to <c>ConsumerDispatchConcurrency</c> on the consumer's RabbitMQ channel.
    /// Defaults to <c>1</c> (sequential dispatch) when not configured.
    /// <para>
    /// Must be called after <see cref="AddRabbitMqTransport"/> and after the consumer has
    /// been registered.
    /// </para>
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to configure.</typeparam>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="concurrencyCount">
    /// The number of concurrent message handlers. Must be at least <c>1</c>.
    /// </param>
    public static IInterBusBuilder WithConsumerConcurrency<TConsumer>(
        this IInterBusBuilder builder,
        ushort concurrencyCount)
        where TConsumer : class, IConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (concurrencyCount == 0)
            throw new ArgumentOutOfRangeException(nameof(concurrencyCount), "Concurrency count must not be zero.");

        var descriptor = builder.Services.FirstOrDefault(
            d => d.ServiceType == typeof(RabbitMqTransportOptions));

        if (descriptor?.ImplementationInstance is RabbitMqTransportOptions options)
        {
            var queueName = RoutingConventions.QueueName(typeof(TConsumer));
            if (!options.QueueOptions.TryGetValue(queueName, out var queueOptions))
            {
                queueOptions = new RabbitMqQueueOptions();
                options.QueueOptions[queueName] = queueOptions;
            }
            queueOptions.ConcurrencyCount = concurrencyCount;
        }

        return builder;
    }
}
