using InterBus.Middleware;
using Microsoft.Extensions.Logging;

namespace InterBus;

/// <summary>
/// Default outermost consume middleware. Catches all non-cancellation exceptions thrown by
/// downstream pipeline steps (including the consumer itself) and logs them as errors
/// </summary>
public sealed class ExceptionLoggingConsumeMiddleware(ILogger<ExceptionLoggingConsumeMiddleware> logger)
    : IConsumeMiddleware
{
    public async Task InvokeAsync(
        ConsumeContext context,
        Func<ConsumeContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await next(context, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                ex,
                "InterBus: unhandled exception consuming message for consumer {ConsumerType}",
                context.Descriptor.ConsumerType.Name);
        }
    }
}
