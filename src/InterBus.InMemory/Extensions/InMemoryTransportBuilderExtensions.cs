using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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

    /// <summary>
    /// Registers an <see cref="IRequestClient{TRequest,TReply}"/> that sends requests directly
    /// to the queue of <typeparamref name="TConsumer"/> and waits for a reply of type
    /// <typeparamref name="TReply"/> on a temporary queue.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer that will handle the request.</typeparam>
    /// <typeparam name="TRequest">The request message type.</typeparam>
    /// <typeparam name="TReply">The expected reply message type.</typeparam>
    public static InterBusBuilder AddRequestClient<TConsumer, TRequest, TReply>(this InterBusBuilder builder)
        where TConsumer : IConsumer<TRequest>
        where TRequest : class
        where TReply : class
    {
        var destination = RoutingConventions.QueueUri(builder.Options.QueueUriScheme, typeof(TConsumer));

        builder.Services.AddScoped<IRequestClient<TRequest, TReply>>(sp =>
            new InMemoryRequestClient<TRequest, TReply>(
                sp.GetRequiredService<InMemoryBroker>(),
                sp.GetRequiredService<IServiceScopeFactory>(),
                sp.GetRequiredService<InterBusOptions>(),
                destination,
                sp.GetRequiredService<ILogger<InMemoryRequestClient<TRequest, TReply>>>()));

        return builder;
    }
}
