namespace InterBus.Middleware;

public interface IMiddlewareFactory
{
    public IEnumerable<TMiddleware> Resolve<TMiddleware>(Type? key = null) where TMiddleware : IMiddleware;
}
