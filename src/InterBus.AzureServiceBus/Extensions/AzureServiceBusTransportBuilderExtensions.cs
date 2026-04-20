using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Extensions;

/// <summary>
/// Extension methods for configuring the Azure Service Bus transport on an
/// <see cref="IInterBusBuilder"/>.
/// </summary>
public static class AzureServiceBusTransportBuilderExtensions
{
    /// <param name="builder">The InterBus builder.</param>
    extension(IInterBusBuilder builder)
    {
        /// <summary>
        /// Use an Azure Service Bus backed transport for message publishing and consumption.
        /// <para>
        /// Topic (fan-out) messages are published to an Azure Service Bus topic. Each registered
        /// consumer that handles the message type has a subscription on that topic with
        /// <c>ForwardTo</c> set to the consumer's dedicated queue, so every consumer receives a copy.
        /// </para>
        /// <para>
        /// Queue (point-to-point) messages are sent directly to a consumer's Azure Service Bus queue,
        /// delivering the message to exactly one consumer.
        /// </para>
        /// <para>
        /// Delayed delivery is natively supported: setting a delay routes the message through
        /// Azure Service Bus's scheduled-enqueue mechanism, holding it until the scheduled time.
        /// </para>
        /// <para>
        /// Topology (topics, queues, subscriptions) is declared automatically at startup using
        /// <see cref="Azure.Messaging.ServiceBus.Administration.ServiceBusAdministrationClient"/>.
        /// Entities that already exist are left unchanged.
        /// </para>
        /// </summary>
        /// <param name="configure">
        /// Delegate to configure <see cref="AzureServiceBusTransportOptions"/>. At minimum set
        /// <see cref="AzureServiceBusTransportOptions.ConnectionString"/>.
        /// </param>
        public IInterBusBuilder AddAzureServiceBusTransport(Action<AzureServiceBusTransportOptions>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(builder);

            var options = new AzureServiceBusTransportOptions();
            configure?.Invoke(options);

            builder.Options.SetTransportUriSchemes(
                AzureServiceBusTopicTransport.TopicScheme,
                AzureServiceBusQueueTransport.QueueScheme);

            builder.Services.AddSingleton(options);

            builder.Services.AddSingleton<AzureServiceBusClientProvider>();
            builder.Services.AddSingleton<IInterBusTopologyBuilder, AzureServiceBusTopology>();

            builder.Services.AddKeyedScoped<IInterBusTransport>(AzureServiceBusTopicTransport.TopicScheme, (sp, _) =>
            {
                var topology = sp.GetServices<IInterBusTopologyBuilder>().OfType<AzureServiceBusTopology>().Single();
                return new AzureServiceBusTopicTransport(
                    sp.GetRequiredService<AzureServiceBusClientProvider>(),
                    topology);
            });
            builder.Services.AddKeyedScoped<IInterBusTransport>(AzureServiceBusQueueTransport.QueueScheme, (sp, _) =>
            {
                var topology = sp.GetServices<IInterBusTopologyBuilder>().OfType<AzureServiceBusTopology>().Single();
                return new AzureServiceBusQueueTransport(
                    sp.GetRequiredService<AzureServiceBusClientProvider>(),
                    topology);
            });

            builder.Services.AddHostedService<AzureServiceBusMessagePump>();

            return builder;
        }
    }
}
