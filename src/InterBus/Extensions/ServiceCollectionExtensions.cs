using System.Diagnostics.CodeAnalysis;
using InterBus.Internal;
using InterBus.Middleware;
using InterBus.Middleware.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Extensions;

/// <summary>Extension methods for registering InterBus with <see cref="IServiceCollection"/>.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Add the InterBus core services and configure consumers via the <paramref name="configure"/> delegate.
    /// Call a transport extension method (e.g. <c>AddInMemoryTransport</c>) inside <paramref name="configure"/>
    /// before the host is built.
    /// <para>
    /// A <see cref="SystemTextJsonMessageSerializer"/> backed by a reflection-based JSON resolver is
    /// registered as the default <see cref="IMessageSerializer"/>. Override it inside
    /// <paramref name="configure"/> by calling
    /// <c>bus.UseSerializer&lt;TSerializer&gt;()</c> or
    /// <c>bus.Services.AddSingleton&lt;IMessageSerializer&gt;(instance)</c> for AOT-safe
    /// or custom serialisation.
    /// </para>
    /// </summary>
    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026",
        Justification =
            "SystemTextJsonMessageSerializer is registered as the default IMessageSerializer and uses reflection-based JSON. " +
            "Override with bus.UseSerializer<TSerializer>() or bus.Services.AddSingleton<IMessageSerializer>(instance) for AOT/trim applications.")]
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050",
        Justification =
            "SystemTextJsonMessageSerializer is registered as the default IMessageSerializer and requires dynamic code. " +
            "Override with bus.UseSerializer<TSerializer>() or bus.Services.AddSingleton<IMessageSerializer>(instance) for AOT applications.")]
    public static IServiceCollection AddInterBus(
        this IServiceCollection services,
        Action<IInterBusBuilder> configure)
    {
        var options = new InterBusOptions();
        var builder = new InterBusBuilder(services, options);

        configure(builder);

        // Register options so transports can resolve them.
        services.AddSingleton(options);

        // Register the default serialiser if the user has not supplied one via UseSerializer<T>()
        // or a direct services.AddSingleton<IMessageSerializer>(...) call inside configure
        services.TryAddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();

        services.AddScoped<IMiddlewareFactory, MiddlewareFactory>();
        services.AddScoped<IConsumeContextAccessor, ConsumeContextAccessor>();

        services.AddScoped<DispatchPipelineExecutor>();
        services.AddScoped<ConsumePipelineExecutor>();
        services.AddScoped<SchedulePipelineExecutor>();
        services.AddScoped<SendPipelineExecutor>();

        // Register the transport factory and the scoped publisher.
        services.AddSingleton<IInterBusTransportFactory, InterBusTransportFactory>();
        services.AddScoped<IMessagePublisher, BusMessagePublisher>();

        // Register final steps in middleware pipelines
        builder.UsePublishMiddleware<PublishToSendPipeline>();

        return services;
    }
}
