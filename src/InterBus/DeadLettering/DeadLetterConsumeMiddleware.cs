using InterBus.Consumer;

namespace InterBus.DeadLettering;

/// <summary>
/// Outermost consume middleware that marks unhandled consumer exceptions for dead-lettering
/// and publishes a <see cref="DeadLetter{T}"/> message to allow application consumers to
/// receive and process dead-lettered messages.
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
        catch (DeadLetterException)
        {
            throw;
        }
        catch (Exception ex)
        {
            var deadLetteredAtUtc = DateTimeOffset.UtcNow;
            const string reason = "Unhandled exception in consume pipeline";

            context.Headers[InterBusHeaders.DeadLetterReason] = reason;
            context.Headers[InterBusHeaders.DeadLetterError] = ex.ToString();
            context.Headers[InterBusHeaders.DeadLetteredAt] = deadLetteredAtUtc.ToString("O");

            // Publish a DeadLetter<T> event so application consumers can subscribe to failed messages.
            // Skip publishing if T is already a DeadLetter<> type to prevent infinite nesting.
            if (!typeof(T).IsGenericType || typeof(T).GetGenericTypeDefinition() != typeof(DeadLetter<>))
            {
                var deadLetter = new DeadLetter<T>
                {
                    OriginalMessage = context.Message,
                    Reason = reason,
                    Error = ex.ToString(),
                    DeadLetteredAtUtc = deadLetteredAtUtc,
                };

                try
                {
                    // Use CancellationToken.None so a cancelled consumer token does not suppress
                    // the dead-letter publish.
                    await publisher.PublishAsync(deadLetter, CancellationToken.None);
                }
                catch
                {
                    // Swallow publish errors to avoid masking the original consumer exception.
                }
            }

            throw new DeadLetterException(reason, ex.ToString(), deadLetteredAtUtc, ex);
        }
    }
}
