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

        // Register the exception-logging consume middleware as the outermost (first) step
        // before user-defined middleware so it catches exceptions from all downstream steps.
        services.AddScoped<IConsumeMiddleware, ExceptionLoggingConsumeMiddleware>();

        configure(builder);

        // Register options so transports can resolve them.
        services.AddSingleton(options);

        // Register the consume pipeline executor (scoped: one per message dispatch scope).
        services.AddScoped<ConsumePipelineExecutor>();

        // Register the scoped publisher.
        services.AddScoped<IMessagePublisher, MessagePublisher>();

        return services;
    }
}
