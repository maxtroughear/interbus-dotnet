using InterBus.Middleware;

namespace InterBus.Send;

public interface ISendMiddleware : IMiddleware
{
    Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default);
}
