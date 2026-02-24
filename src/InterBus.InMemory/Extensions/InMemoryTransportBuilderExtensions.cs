using Microsoft.Extensions.DependencyInjection;

namespace InterBus.InMemory.Extensions;

/// <summary>Extension methods for configuring the in-memory transport on an <see cref="IInterBusBuilder"/>.</summary>
public static class InMemoryTransportBuilderExtensions
{
    /// <summary>
    /// Use the in-memory <see cref="System.Threading.Channels"/>-backed transport.
    /// Suitable for testing and single-process applications.
    /// </summary>
    public static IInterBusBuilder AddInMemoryTransport(this IInterBusBuilder builder)
    {
        // Broker is singleton: owns all topics and queues for the lifetime of the application.
        builder.Services.AddSingleton<InMemoryBroker>();
        builder.Services.AddSingleton<IInterBusTransport>(sp => sp.GetRequiredService<InMemoryBroker>());

        // Message pump runs as a background service.
        builder.Services.AddHostedService<InMemoryMessagePump>();

        return builder;
    }
}
