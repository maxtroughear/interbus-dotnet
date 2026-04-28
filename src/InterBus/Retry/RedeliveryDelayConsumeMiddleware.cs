using System.Globalization;
using InterBus.Consuming;
using InterBus.Transporting;
using Microsoft.Extensions.Logging;

namespace InterBus.Retry;

/// <summary>
/// Consume middleware that retries by re-sending the failed message back to the same transport queue
/// using the transport's native delay mechanism (<see cref="TransportSendContext.Delay"/>) for
/// configurable retry intervals, without requiring a separate scheduling infrastructure.
/// </summary>
/// <remarks>
/// Configure via <see cref="IInterBusBuilder"/> using <c>UseRedeliveryTransportDelayMiddleware</c>
/// and supply <see cref="RedeliveryOptions"/> to control retry count, delay strategy,
/// and exception filtering.
/// Transports that do not support deferred visibility will ignore the delay and deliver immediately.
/// </remarks>
public sealed class RedeliveryDelayConsumeMiddleware(
    RedeliveryOptions options,
    IPublisher publisher,
    ILogger<RedeliveryDelayConsumeMiddleware> logger) : IConsumeMiddleware
{
    /// <summary>
    /// Header key storing the number of transport-delay redelivery retries already attempted for a message.
    /// </summary>
    public const string RetryCountHeader = "interbus-redelivery-delay-retry-count";

    /// <inheritdoc />
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
            var delay = options.DelayProvider(nextAttempt);

            logger.LogWarning(
                ex,
                "InterBus: transport delay redelivery attempt {Attempt}/{MaxAttempts} for consumer {ConsumerType} on queue {QueueName}",
                nextAttempt,
                options.RetryCount,
                context.Descriptor.ConsumerType.Name,
                context.Descriptor.Queue);

            var redeliveryHeaders = new MessageHeaders(context.Headers)
            {
                [RetryCountHeader] = nextAttempt.ToString(CultureInfo.InvariantCulture)
            };

            await publisher.SendAsync(
                context.Descriptor.Queue,
                context.Message,
                ctx =>
                {
                    foreach (var header in redeliveryHeaders)
                        ctx.Headers[header.Key] = header.Value;

                    if (delay > TimeSpan.Zero)
                        ctx.Delay = delay;
                },
                cancellationToken);

            // Swallow to signal the current delivery has been handled by transport-delay redelivery.
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
