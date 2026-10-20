using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.InMemory.Extensions;

/// <summary>Extension methods for configuring the in-memory transport on an <see cref="InterBusBuilder"/>.</summary>
public static class InMemoryTransportBuilderExtensions
{
    extension(InterBusBuilder builder)
    {
        /// <summary>
        /// Use the in-memory <see cref="System.Threading.Channels"/>-backed transport.
        /// Suitable for testing and single-process applications.
        /// <para>
        /// Registers <c><see cref="InMemoryBroker.TopicScheme">memorytopic</see>://</c> and
        /// <c><see cref="InMemoryBroker.QueueScheme">memoryqueue</see>://</c> as the destination
        /// URI schemes for topic and queue messages respectively.
        /// </para>
        /// <para>
        /// Also registers <see cref="IReplyEndpointFactory"/> so that
        /// <see cref="IRequestClient{TRequest,TReply}"/> works without any additional configuration.
        /// </para>
        /// </summary>
        public InterBusBuilder AddInMemoryTransport()
        {
            // Register the URI schemes so MessagePublisher constructs the correct destination URIs.
            builder.Options.SetTransportUriSchemes(InMemoryBroker.TopicScheme, InMemoryBroker.QueueScheme);

            // Broker is singleton: owns all topics and queues for the lifetime of the application.
            builder.Services.AddSingleton<InMemoryBroker>();
            builder.Services.AddSingleton<ITopologyBuilder, InMemoryTopologyBuilder>();

            // Register the transport as a keyed service for each supported URI scheme so the
            // transport factory can resolve it by destination scheme.
            builder.Services.AddKeyedSingleton<ITransport, InMemoryTransport>(InMemoryBroker.TopicScheme);
            builder.Services.AddKeyedSingleton<ITransport, InMemoryTransport>(InMemoryBroker.QueueScheme);

            // Message pump runs as a background service.
            builder.Services.AddSingleton<InterBus.MessagePumps.IConsumerSourceFactory, InMemorySourceFactory>();

            // Reply endpoint factory enables IRequestClient<,> without additional registration.
            builder.Services.AddSingleton<IReplyEndpointFactory, InMemoryReplyEndpointFactory>();

            return builder;
        }

        /// <summary>
        /// Use the in-memory scheduler. Suitable for testing and single-process applications that don't need
        /// durability.
        /// </summary>
        public InterBusBuilder AddInMemoryScheduler()
        {
            builder.Services.AddSingleton<InMemoryMessageScheduler>();
            builder.Services.AddSingleton<IMessageSchedulingProvider>(sp =>
                sp.GetRequiredService<InMemoryMessageScheduler>());
            builder.Services.AddHostedService(sp => sp.GetRequiredService<InMemoryMessageScheduler>());
            return builder;
        }
    }
}
