using InterBus.Sending;
using InterBus.Tests.Common;

namespace InterBus.DynamoDB.Tests;

internal sealed class FailFirstSendMiddleware(FailFirstSendState state) : ISendMiddleware
{
    public Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next, CancellationToken cancellationToken = default)
    {
        if (!context.Destination.ToString().Contains(nameof(OrderShipped), StringComparison.OrdinalIgnoreCase))
            return next(context, cancellationToken);
        if (Interlocked.Increment(ref state.CallCount) == 1)
            throw new Exception("Simulated send failure");
        return next(context, cancellationToken);
    }
}
