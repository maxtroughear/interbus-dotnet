using InterBus.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Middleware.Internal;

internal sealed class MiddlewareFactory(IServiceProvider serviceProvider) : IMiddlewareFactory
{
    public IEnumerable<TMiddleware> Resolve<TMiddleware>(Type key) where TMiddleware : IMiddleware
    {
        return serviceProvider.GetKeyedServices<TMiddleware>(key).Reverse();
    }

    public IEnumerable<TMiddleware> Resolve<TMiddleware>() where TMiddleware : IMiddleware
    {
        return serviceProvider.GetKeyedServices<TMiddleware>(InterBusConstants.BusContext).Reverse();
    }
}
