using InterBus.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Middleware.Internal;

internal sealed class MiddlewareFactory(IServiceProvider serviceProvider) : IMiddlewareFactory
{
    public IEnumerable<TMiddleware> Resolve<TMiddleware>(Type? key = null) where TMiddleware : IMiddleware
    {
        return key is not null
            ? serviceProvider.GetKeyedServices<TMiddleware>(key).Reverse().DistinctBy(x => x.GetType())
            : serviceProvider.GetKeyedServices<TMiddleware>(InterBusConstants.BusContext).Reverse()
                .DistinctBy(x => x.GetType());
    }
}
