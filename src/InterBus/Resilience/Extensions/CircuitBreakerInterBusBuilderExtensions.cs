using InterBus.Consuming;
using InterBus.Resilience;
using Microsoft.Extensions.DependencyInjection;

// ReSharper disable once CheckNamespace
namespace InterBus;

/// <summary>
/// Extension methods for adding dispatch circuit-breaker middleware.
/// </summary>
public static class CircuitBreakerInterBusBuilderExtensions
{
    /// <summary>
    /// Adds <see cref="CircuitBreakerDispatchMiddleware"/> to the dispatch pipeline.
    /// This middleware is typically added before retry middleware to fail fast while open.
    /// </summary>
    public static ConsumerBuilder UseCircuitBreakerMiddleware(
        this ConsumerBuilder builder,
        Action<CircuitBreakerOptions>? configure = null)
    {
        var options = new CircuitBreakerOptions();
        configure?.Invoke(options);
        options.Validate();

        builder.Services.AddKeyedSingleton(
            builder.Descriptor.ConsumerType,
            (_, _) => new CircuitBreakerState(options));

        return builder.UseDispatchMiddleware<CircuitBreakerDispatchMiddleware>();
    }
}
