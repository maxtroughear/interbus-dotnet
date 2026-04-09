namespace InterBus.Middleware;

public interface IMiddlewareFactory
{
    public IEnumerable<TMiddleware> Resolve<TMiddleware>(Type key) where TMiddleware : IMiddleware;

    public IEnumerable<TMiddleware> Resolve<TMiddleware>() where TMiddleware : IMiddleware;
}
