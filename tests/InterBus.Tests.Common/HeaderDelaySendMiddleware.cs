using InterBus.Sending;

namespace InterBus.Tests.Common;

public sealed class HeaderDelaySendMiddleware : ISendMiddleware
{
    public const string DelayHeaderName = "x-interbus-test-send-delay-ms";
    public async Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next, CancellationToken cancellationToken = default)
    {
        if (context.Headers.TryGetValue(DelayHeaderName, out var delayHeader) && int.TryParse(delayHeader, out var delayMs) && delayMs > 0)
            await Task.Delay(delayMs, cancellationToken);
        await next(context, cancellationToken);
    }
}
