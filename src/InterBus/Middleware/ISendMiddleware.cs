namespace InterBus.Middleware;

public interface ISendMiddleware
{
    Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}
