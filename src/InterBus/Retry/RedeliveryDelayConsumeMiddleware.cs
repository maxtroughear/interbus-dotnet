using InterBus.Middleware;
using Microsoft.Extensions.Logging;

namespace InterBus.Retry;

public sealed class RedeliveryDelayConsumeMiddleware(
    RedeliveryOptions options,
    IInterBusTransport transport,
    ILogger<RedeliveryDelayConsumeMiddleware> logger) : IConsumeMiddleware
{
    public Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        throw new NotImplementedException();
    }
}
