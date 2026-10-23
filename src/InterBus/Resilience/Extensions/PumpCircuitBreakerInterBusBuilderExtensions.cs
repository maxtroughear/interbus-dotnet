using InterBus.Consuming;
using InterBus.Resilience;
using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus;

/// <summary>Configures consumer-wide circuit admission without rejecting accepted deliveries.</summary>
public static class PumpCircuitBreakerInterBusBuilderExtensions
{
    /// <summary>Enables dependency and/or transport intake policies for this consumer.</summary>
    /// <param name="builder">Consumer to configure.</param>
    /// <param name="configure">Optional policy settings.</param>
    /// <returns>The consumer builder.</returns>
    /// <exception cref="InvalidOperationException">Dispatch-only circuit middleware is already configured.</exception>
    public static ConsumerBuilder UsePumpCircuitBreaker(this ConsumerBuilder builder, Action<PumpCircuitBreakerOptions>? configure = null)
    {
        var options = new PumpCircuitBreakerOptions();
        configure?.Invoke(options);
        options.Validate();
        ValidateConfiguration(builder.Services, builder.Descriptor, enabling: true);
        builder.Descriptor.PumpCircuitBreaker = options;
        return builder;
    }

    internal static void ValidateConfiguration(IServiceCollection services, ConsumerDescriptor descriptor, bool enabling = false)
    {
        if ((enabling || descriptor.PumpCircuitBreaker is not null) && services.Any(service =>
            service.IsKeyedService && service.ServiceType == typeof(IDispatchMiddleware) &&
            Equals(service.ServiceKey, descriptor.ConsumerType) &&
            service.KeyedImplementationType == typeof(CircuitBreakerDispatchMiddleware)))
            throw new InvalidOperationException("UsePumpCircuitBreaker cannot be combined with UseCircuitBreakerMiddleware: dispatch-only admission can reject accepted drain work and recovery probes");
    }
}
