using System.Text.Json;
using InterBus.Consuming;
using InterBus.Dispatching;

namespace InterBus.DeadLettering;

/// <summary>
/// Outermost consume middleware that marks unhandled consumer exceptions for dead-lettering
/// and publishes a <see cref="DeadLetter{T}"/> message to allow application consumers to
/// receive and process dead-lettered messages.
/// <para>
/// The dead-letter notification is published via <see cref="IPublisher"/>. When the transactional
/// outbox is configured on the consumer, the outbox publish middleware detects
/// <see cref="DeadLetter{T}"/> messages and forwards them directly to the transport, bypassing
/// the outbox buffer — ensuring delivery even when no <c>SaveChangesAsync</c> is called in the
/// failure path.
/// </para>
/// </summary>
public sealed class DeadLetterConsumeMiddleware(IPublisher publisher) : IConsumeMiddleware
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var deadLetteredAtUtc = DateTimeOffset.UtcNow;

            context.Result = DispatchResult.DeadLetter(deadLetteredAtUtc, ex);

            // Publish a DeadLetter<T> event so application consumers can subscribe to failed messages.
            // Skip publishing if T is already a DeadLetter<> type to prevent infinite nesting.
            if (!typeof(T).IsGenericType || typeof(T).GetGenericTypeDefinition() != typeof(DeadLetter<>))
            {
                var deadLetter = new DeadLetter<T>
                {
                    OriginalMessage = context.Message,
                    OriginalHeaders = context.Headers,
                    Reason = context.Result.DeadLetterReason,
                    Error = context.Result.DeadLetterError,
                    DeadLetteredAtUtc = deadLetteredAtUtc,
                };

                try
                {
                    // Use CancellationToken.None so a cancelled consumer token does not suppress
                    // the dead-letter publish.
                    await publisher.PublishAsync(deadLetter, publishContext =>
                    {
                        publishContext.Headers[InterBusHeaders.DeadLetterReason] = context.Result.DeadLetterReason;
                        publishContext.Headers[InterBusHeaders.DeadLetterError] = context.Result.DeadLetterError;
                        publishContext.Headers[InterBusHeaders.DeadLetterTime] = deadLetteredAtUtc.ToString("O");
                    }, CancellationToken.None);
                }
                catch
                {
                    // Swallow publish errors to avoid masking the original consumer exception.
                }
            }
        }
    }
}
