using InterBus.Consumer;

namespace InterBus.DeadLettering;

/// <summary>
/// Outermost consume middleware that marks unhandled consumer exceptions for dead-lettering.
/// </summary>
public sealed class DeadLetterConsumeMiddleware : IConsumeMiddleware
{
    public Task InvokeAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class =>
        InvokeCoreAsync(context, next, cancellationToken);

    private static async Task InvokeCoreAsync<T>(
        ConsumeContext<T> context,
        Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken) where T : class
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

            throw new DeadLetterException(reason, ex.ToString(), deadLetteredAtUtc, ex);
        }
    }
}
