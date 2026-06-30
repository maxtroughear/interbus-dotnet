using InterBus.Compression;
using InterBus.Consuming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

// ReSharper disable once CheckNamespace
namespace InterBus;

/// <summary>
/// Extension methods for enabling payload compression middleware.
/// </summary>
public static class CompressionInterBusBuilderExtensions
{
    extension(InterBusBuilder builder)
    {
        /// <summary>
        /// Registers payload compression for outgoing bus messages.
        /// </summary>
        /// <param name="configure">Optional compression configuration.</param>
        public InterBusBuilder AddCompression(Action<CompressionOptions>? configure = null)
        {
            var options = new CompressionOptions();
            configure?.Invoke(options);

            builder.Services.Replace(ServiceDescriptor.Singleton(options));
            builder.UseSendMiddleware<CompressionSendMiddleware>();
            return builder;
        }
    }

    extension(ConsumerBuilder builder)
    {
        /// <summary>
        /// Registers compression middleware for this consumer.
        /// </summary>
        public ConsumerBuilder UseCompression()
        {
            builder.Services.TryAddSingleton(new CompressionOptions());
            builder.UseSendMiddleware<CompressionSendMiddleware>();
            builder.UseDispatchMiddleware<CompressionDispatchMiddleware>();
            return builder;
        }
    }
}
