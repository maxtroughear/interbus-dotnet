using System.Globalization;
using InterBus.Middleware;
using Microsoft.Extensions.Logging;

namespace InterBus.Retry;

public sealed class RedeliveryImmediateConsumeMiddleware(
    RedeliveryOptions options,
    IMessagePublisher publisher,
    ILogger<RedeliveryImmediateConsumeMiddleware> logger) : IConsumeMiddleware
{
    public const string RetryCountHeader = "interbus-redelivery-immediate-retry-count";

    public async Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next,
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
                "InterBus: redelivery attempt {Attempt}/{MaxAttempts} for consumer {ConsumerType} on queue {QueueName}",
                nextAttempt,
                options.RetryCount,
                context.Descriptor.ConsumerType.Name,
                context.Descriptor.QueueName);

            var redeliveryHeaders = new MessageHeaders(context.Headers)
            {
                [RetryCountHeader] = nextAttempt.ToString(CultureInfo.InvariantCulture)
            };

            await publisher.Send(
                context.Descriptor.QueueName,
                context.Payload,
                redeliveryHeaders,
                cancellationToken);

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
