using InterBus.Middleware;

namespace InterBus.Dispatch;

public interface IDispatchMiddleware : IMiddleware
{
    Task InvokeAsync(DispatchContext context, Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}
