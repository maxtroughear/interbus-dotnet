using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Resilience;

/// <summary>
/// Dispatch middleware that opens a circuit when failure percentage exceeds a configured
/// threshold after minimum throughput is reached, then short-circuits while open.
/// </summary>
public sealed class CircuitBreakerDispatchMiddleware(IServiceProvider serviceProvider) : IDispatchMiddleware
{
    public async Task InvokeAsync(
        DispatchContext context,
        Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        var state = serviceProvider.GetKeyedService<CircuitBreakerState>(context.Descriptor.ConsumerType);
        if (state is null)
        {
            await next(context, cancellationToken);
            return;
        }

        var now = DateTimeOffset.UtcNow;
        if (state.IsOpen(now))
        {
            context.Result =
                DispatchResult.Release(state.CreateOpenCircuitException(context.Descriptor.ConsumerType.Name));
            return;
        }

        await next(context, cancellationToken);

        state.RecordOutcome(context.Result.Kind is not DispatchResultKind.Success, now);
    }
}
