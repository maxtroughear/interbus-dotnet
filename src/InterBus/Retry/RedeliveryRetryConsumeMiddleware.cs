using System.Globalization;
using InterBus.Middleware;
using Microsoft.Extensions.Logging;

namespace InterBus.Retry;

/// <summary>
/// Consume middleware that schedules retry attempts by redelivering the failed message
/// back to the same transport queue.
/// </summary>
/// <remarks>
/// Configure via <see cref="IInterBusBuilder"/> using <c>UseRedeliveryRetryMiddleware</c>
/// and supply <see cref="RedeliveryRetryOptions"/> to control retry count, delay strategy,
/// and exception filtering.
/// </remarks>
public sealed class RedeliveryRetryConsumeMiddleware(
    RedeliveryRetryOptions options,
    IInterBusTransport transport,
    IMessageSchedulingProvider? schedulingProvider,
    ILogger<RedeliveryRetryConsumeMiddleware> logger) : IConsumeMiddleware
{
    /// <summary>
    /// Header key storing the number of redelivery retries already scheduled for a message.
    /// </summary>
    public const string RetryCountHeader = "interbus-redelivery-retry-count";

    /// <inheritdoc />
    public async Task InvokeAsync(
        ConsumeContext context,
        Func<ConsumeContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await next(context, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ShouldRetry(ex) && TryGetNextAttempt(context.Headers, out var nextAttempt))
        {
            logger.LogWarning(
                ex,
                "InterBus: scheduling transport redelivery attempt {Attempt}/{MaxAttempts} for consumer {ConsumerType} on queue {QueueName}",
                nextAttempt,
                options.RetryCount,
                context.Descriptor.ConsumerType.Name,
                context.Descriptor.QueueName);

            var delay = options.DelayProvider(nextAttempt);

            var redeliveryHeaders = new MessageHeaders(context.Headers)
            {
                [RetryCountHeader] = nextAttempt.ToString(CultureInfo.InvariantCulture)
            };

            if (delay > TimeSpan.Zero && schedulingProvider != null)
            {
                await schedulingProvider.ScheduleSendToQueueAsync(
                    context.Descriptor.QueueName,
                    context.Payload,
                    redeliveryHeaders,
                    DateTimeOffset.UtcNow + delay,
                    cancellationToken);
            }
            else
            {
                await transport.SendToQueueAsync(
                    context.Descriptor.QueueName,
                    context.Payload,
                    redeliveryHeaders,
                    cancellationToken);
            }

            // Swallow to signal the current delivery has been handled by scheduling redelivery.
        }
    }

    private bool ShouldRetry(Exception ex) =>
        options.ShouldRetry is null || options.ShouldRetry(ex);

    private bool TryGetNextAttempt(MessageHeaders headers, out int nextAttempt)
    {
        if (headers.TryGetValue(RetryCountHeader, out var raw) &&
            int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var currentAttempt) &&
            currentAttempt >= 0)
        {
            nextAttempt = currentAttempt + 1;
        }
        else
        {
            nextAttempt = 1;
        }

        return nextAttempt <= options.RetryCount;
    }
}
