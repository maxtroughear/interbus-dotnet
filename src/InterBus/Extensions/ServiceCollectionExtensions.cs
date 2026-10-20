using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Internal;
using InterBus.MessagePumps;
using InterBus.Middleware;
using InterBus.Middleware.Internal;
using InterBus.Publishing;
using InterBus.Requests;
using InterBus.Sending;
using InterBus.Serialization;
using InterBus.Telemetry;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Extensions;

/// <summary>Extension methods for registering InterBus with <see cref="IServiceCollection"/>.</summary>
public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
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
                "Override with bus.UseSerializer<TSerializer>() or bus.Services.AddSingleton<IMessageSerializer>(instance) for AOT/trim applications")]
        [UnconditionalSuppressMessage("AotAnalysis", "IL3050",
            Justification =
                "SystemTextJsonMessageSerializer is registered as the default IMessageSerializer and requires dynamic code. " +
                "Override with bus.UseSerializer<TSerializer>() or bus.Services.AddSingleton<IMessageSerializer>(instance) for AOT applications")]
        public IServiceCollection AddInterBus(Action<InterBusBuilder> configure)
        {
            var options = new InterBusOptions();
            var builder = new InterBusBuilder(services, options);

            services.AddHostedService<InterBusTopologyHostedService>();

            // Telemetry (ActivitySource + Meter) — registered before anything that uses them.
            services.AddSingleton<InterBusDiagnostics>();
            services.AddSingleton<IInterBusDiagnostics>(sp => sp.GetRequiredService<InterBusDiagnostics>());
            services.AddSingleton<InterBusMetrics>();

            // Register the bus-level telemetry publish middleware first so it is outermost for
            // standalone publishes (outside a consumer context).
            services.AddKeyedScoped<IPublishMiddleware, TelemetryPublishMiddleware>(InterBusConstants.BusContext);

            configure(builder);

            if (services.Any(service => service.ServiceType == typeof(IConsumerSourceFactory)))
            {
                services.AddSingleton<PumpController>();
                services.AddHostedService<PumpHost>();
            }

            services.AddSingleton(options);

            // Register the default serialiser if the user has not supplied one via UseSerializer<T>()
            // or a direct services.AddSingleton<IMessageSerializer>(...) call inside configure
            services.TryAddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();

            services.AddScoped<IMiddlewareFactory, MiddlewareFactory>();
            services.AddScoped<ConsumeContextAccessor>();

            services.AddSingleton<Dispatcher>();
            services.AddSingleton<DeliveryProcessor>();
            
            services.AddScoped<DispatchPipelineExecutor>();
            services.AddScoped<ConsumePipelineExecutor>();
            services.AddScoped<PublishPipelineExecutor>();
            services.AddScoped<SendPipelineExecutor>();

            // Register the transport factory and the scoped publisher.
            services.AddSingleton<ITransportFactory, TransportFactory>();
            services.AddScoped<IPublisher, Publisher>();
            
            // Register IRequestClient<,> as an open-generic scoped service.
            // Requires the transport to register IReplyEndpointFactory.
            services.AddScoped(typeof(IRequestClient<,>), typeof(RequestClient<,>));

            return services;
        }
    }
}
