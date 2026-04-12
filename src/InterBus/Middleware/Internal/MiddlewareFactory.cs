using InterBus.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Middleware.Internal;

internal sealed class MiddlewareFactory(IServiceProvider serviceProvider) : IMiddlewareFactory
{
    public IEnumerable<TMiddleware> Resolve<TMiddleware>(Type key) where TMiddleware : IMiddleware
    {
        // Consumer-specific middlewares wrap the all-consumers middlewares, so all-consumers
        // are applied innermost (execute last) and consumer-specific are outermost (execute first).
        // After reversing each set, concatenating allConsumers + specific places allConsumers
        // as the first items in the enumerable (innermost when wrapped in the pipeline loop).
        var specific = serviceProvider.GetKeyedServices<TMiddleware>(key).Reverse();
        var allConsumers = serviceProvider.GetKeyedServices<TMiddleware>(InterBusConstants.AllConsumersContext).Reverse();
        return allConsumers.Concat(specific);
    }

    public IEnumerable<TMiddleware> Resolve<TMiddleware>() where TMiddleware : IMiddleware
    {
        return serviceProvider.GetKeyedServices<TMiddleware>(InterBusConstants.BusContext).Reverse();
    }
}
