using Microsoft.Extensions.Logging;

namespace InterBus.Internal;

/// <summary>
/// Default outermost consume middleware. Catches all non-cancellation exceptions thrown by
/// downstream pipeline steps (including the consumer itself) and logs them as errors.
/// In a future version this step will be extended with in-process retries,
/// transport-level retries, and dead-lettering.
/// </summary>
internal sealed class ExceptionLoggingConsumeMiddleware : IConsumeMiddleware
{
    private readonly ILogger<ExceptionLoggingConsumeMiddleware> _logger;

    public ExceptionLoggingConsumeMiddleware(ILogger<ExceptionLoggingConsumeMiddleware> logger)
    {
        _logger = logger;
    }

    public async Task InvokeAsync(
        ConsumePipelineContext context,
        ConsumeDelegate next,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await next(context, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(
                ex,
                "InterBus: unhandled exception consuming message for consumer {ConsumerType}",
                context.Descriptor.ConsumerType.Name);
        }
    }
}
