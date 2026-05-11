using Microsoft.Extensions.DependencyInjection;

namespace InterBus.InMemory.Extensions;

/// <summary>Extension methods for configuring the in-memory transport on an <see cref="InterBusBuilder"/>.</summary>
public static class InMemoryTransportBuilderExtensions
{
    /// <summary>
    /// Use the in-memory <see cref="System.Threading.Channels"/>-backed transport.
    /// Suitable for testing and single-process applications.
    /// <para>
    /// Registers <c><see cref="InMemoryBroker.TopicScheme">memorytopic</see>://</c> and
    /// <c><see cref="InMemoryBroker.QueueScheme">memoryqueue</see>://</c> as the destination
    /// URI schemes for topic and queue messages respectively.
    /// </para>
    /// </summary>
    public static InterBusBuilder AddInMemoryTransport(this InterBusBuilder builder)
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
        builder.Services.AddHostedService<InMemoryMessagePump>();

        return builder;
    }
}
