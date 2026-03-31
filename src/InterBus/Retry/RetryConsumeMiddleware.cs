using InterBus.Middleware;
using Microsoft.Extensions.Logging;

namespace InterBus.Retry;

/// <summary>
/// Consume middleware that retries message processing in-process before allowing the
/// exception to propagate to outer middleware (e.g. the transport).
/// </summary>
/// <remarks>
/// <para>
/// Configure via <see cref="IInterBusBuilder"/> using the
/// <c>UseRetryMiddleware</c> extension method and supply a <see cref="RetryOptions"/>
/// instance to control the retry count, delay strategy, and exception filter.
/// </para>
/// <para>
/// <see cref="OperationCanceledException"/> is only bypassed (propagated without retrying)
/// when the supplied <see cref="CancellationToken"/> is cancelled.
/// Exceptions thrown by consumer-internal cancellation tokens are retried normally.
/// </para>
/// </remarks>
public sealed class RetryConsumeMiddleware(
    RetryOptions options,
    ILogger<RetryConsumeMiddleware> logger) : IConsumeMiddleware
{
    /// <inheritdoc/>
    public async Task InvokeAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        var attempt = 0;

        while (true)
        {
            try
            {
                await next(context, cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < options.RetryCount && ShouldRetry(ex))
            {
                attempt++;

                logger.LogWarning(
                    ex,
                    "InterBus: exception on attempt {Attempt}/{MaxAttempts} for consumer {ConsumerType}. Retrying.",
                    attempt,
                    options.RetryCount + 1,
                    context.Descriptor.ConsumerType.Name);

                var delay = options.DelayProvider(attempt);
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private bool ShouldRetry(Exception ex) =>
        options.ShouldRetry is null || options.ShouldRetry(ex);
}
