using InterBus.Middleware;

namespace InterBus.Dispatching;

public interface IDispatchMiddleware : IMiddleware
{
    Task InvokeAsync(DispatchContext context, Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}
