using InterBus.Sending;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class FailSendOnceMiddleware(FailSendOnceController controller) : ISendMiddleware
{
    public Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next, CancellationToken cancellationToken = default)
    {
        if (!controller.ShouldFail)
            return next(context, cancellationToken);
        controller.ShouldFail = false;
        throw new InvalidOperationException("Simulated send failure");
    }
}
