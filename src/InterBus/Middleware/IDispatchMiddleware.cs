namespace InterBus.Middleware;

public interface IDispatchMiddleware
{
    Task InvokeAsync(DispatchContext context, Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}
