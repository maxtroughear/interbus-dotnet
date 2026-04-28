using InterBus.Middleware;

namespace InterBus.Sending;

public interface ISendMiddleware : IMiddleware
{
    Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}
