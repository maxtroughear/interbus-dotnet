using InterBus.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Extensions;

/// <summary>Extension methods for registering InterBus with <see cref="IServiceCollection"/>.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Add the InterBus core services and configure consumers via the <paramref name="configure"/> delegate.
    /// Call a transport extension method (e.g. <c>AddInMemoryTransport</c>) inside <paramref name="configure"/>
    /// before the host is built.
    /// </summary>
    public static IServiceCollection AddInterBus(
        this IServiceCollection services,
        Action<IInterBusBuilder> configure)
    {
        var options = new InterBusOptions();
        var builder = new InterBusBuilder(services, options);

        configure(builder);

        // Register options so transports can resolve them.
        services.AddSingleton(options);

        // Register the scoped publisher.
        services.AddScoped<IMessagePublisher, MessagePublisher>();

        return services;
    }
}
