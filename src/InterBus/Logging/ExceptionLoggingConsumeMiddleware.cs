using InterBus.Consuming;
using Microsoft.Extensions.Logging;

namespace InterBus.Logging;

/// <summary>
/// Default outermost consume middleware. Catches all non-cancellation exceptions thrown by
/// downstream pipeline steps (including the consumer itself) and logs them as errors
/// </summary>
public sealed class ExceptionLoggingConsumeMiddleware(ILogger<ExceptionLoggingConsumeMiddleware> logger)
    : IConsumeMiddleware
{
    public async Task InvokeAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
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

            throw;
        }
    }
}
