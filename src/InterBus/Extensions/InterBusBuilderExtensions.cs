using System.Text.Json;
using InterBus.Consumer;
using InterBus.Generated;
using InterBus.Scheduler;
using InterBus.Serialization;
using InterBus.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Extensions;

/// <summary>Extension methods for <see cref="IInterBusBuilder"/>.</summary>
public static class InterBusBuilderExtensions
{
    /// <param name="builder">The InterBus builder.</param>
    extension(IInterBusBuilder builder)
    {
        /// <summary>
        /// Register the InterBus scheduling infrastructure. This enables injecting
        /// <see cref="IScheduler"/> to schedule messages for deferred delivery.
        /// <para>
        /// A concrete <see cref="IMessageSchedulingProvider"/> must also be registered (e.g. via
        /// <c>AddTickerQScheduler()</c>) to supply the underlying delivery mechanism.
        /// </para>
        /// </summary>
        public IInterBusBuilder AddScheduling(Action<ISchedulerBuilder> configure)
        {
            ArgumentNullException.ThrowIfNull(builder);
            builder.Services.AddScoped<IScheduler, Scheduler.Scheduler>();
            
            configure(new SchedulerBuilder(builder, builder.Services));
            
            return builder;
        }

        /// <summary>
        /// Applies a consumer builder configuration delegate to <em>all previously registered</em>
        /// consumers. Useful after <c>AddConsumers()</c> to configure options (such as
        /// <see cref="ConsumerDescriptor.ConcurrencyCount"/>) for every consumer at once.
        /// <para>
        /// Only consumers already registered at the time of this call are affected. Consumers
        /// registered afterwards are not configured by this call.
        /// </para>
        /// </summary>
        /// <param name="configure">
        /// Delegate that receives an <see cref="IConsumerBuilder"/> for each registered consumer
        /// and applies the desired configuration.
        /// </param>
        public IInterBusBuilder WithConsumers(Action<IConsumerBuilder> configure)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(configure);

            foreach (var descriptor in builder.Options.Consumers)
                configure(new ConsumerBuilder(descriptor, builder.Services));

            return builder;
        }

        /// <summary>
        /// Register a <see cref="SystemTextJsonMessageSerializer"/> configured with the provided
        /// <paramref name="options"/>, replacing the default reflection-based serialiser.
        /// <para>
        /// Designed for trimmed or Native AOT applications where a source-generated
        /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> must be supplied:
        /// </para>
        /// <code>
        /// bus.UseSystemTextJsonSerializer(MyAotJsonContext.Default.Options);
        /// </code>
        /// </summary>
        /// <param name="options">
        /// <see cref="JsonSerializerOptions"/> to pass to <see cref="SystemTextJsonMessageSerializer"/>.
        /// Typically <c>MyJsonContext.Default.Options</c> from a source-generated
        /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>.
        /// </param>
        public IInterBusBuilder UseSystemTextJsonSerializer(JsonSerializerOptions options)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(options);

            builder.Services.Replace(
                ServiceDescriptor.Singleton<IMessageSerializer>(
                    new SystemTextJsonMessageSerializer(options)));
            return builder;
        }
    }
}
