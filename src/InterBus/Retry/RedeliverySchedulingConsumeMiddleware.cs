using System.Globalization;
using InterBus.Middleware;
using Microsoft.Extensions.Logging;

namespace InterBus.Retry;

/// <summary>
/// Consume middleware that retries by scheduling the failed message for redelivery back to the same transport queue
/// with configurable delay and retry count
/// </summary>
/// <remarks>
/// Configure via <see cref="IInterBusBuilder"/> using <c>UseRedeliveryRetryMiddleware</c>
/// and supply <see cref="RedeliveryOptions"/> to control retry count, delay strategy,
/// and exception filtering.
/// </remarks>
public sealed class RedeliverySchedulingConsumeMiddleware(
    RedeliveryOptions options,
    IMessageScheduler scheduler,
    IMessagePublisher publisher,
    ILogger<RedeliverySchedulingConsumeMiddleware> logger) : IConsumeMiddleware
{
    /// <summary>
    /// Header key storing the number of redelivery retries already scheduled for a message.
    /// </summary>
    public const string RetryCountHeader = "interbus-redelivery-retry-count";

    /// <inheritdoc />
    public async Task InvokeAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
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

            // replace scheduling provider with a factory to pull either
            // 1. standard delay provider (sets a delay on the message send that is handled by a transport directly)
            // 2. 

            if (delay > TimeSpan.Zero)
            {
                await scheduler.ScheduleSend(
                    context.Descriptor.QueueName,
                    context.Payload,
                    redeliveryHeaders,
                    delay,
                    cancellationToken);
            }
            else
            {
                await publisher.Send(
                    context.Descriptor.QueueName,
                    context.Payload,
                    ctx =>
                    {
                        foreach (var header in redeliveryHeaders)
                            ctx.Headers[header.Key] = header.Value;
                    },
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
