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
            context.Result = DispatchResult.Release(state.CreateOpenCircuitException(context.Descriptor.ConsumerType.Name));
            return;
        }

        await next(context, cancellationToken);

        state.RecordOutcome(context.Result.Kind is not DispatchResultKind.Success, now);
    }
}

internal sealed class CircuitBreakerState(CircuitBreakerOptions options)
{
    private readonly object _sync = new();
    private readonly Queue<CircuitBreakerOutcome> _outcomes = new();
    private DateTimeOffset _openUntilUtc;
    private int _failuresInWindow;

    public bool IsOpen(DateTimeOffset nowUtc)
    {
        lock (_sync)
            return nowUtc < _openUntilUtc;
    }

    public void RecordOutcome(bool isFailure, DateTimeOffset nowUtc)
    {
        lock (_sync)
        {
            PruneWindow(nowUtc);

            _outcomes.Enqueue(new CircuitBreakerOutcome(nowUtc, isFailure));
            if (isFailure)
                _failuresInWindow++;

            var totalInWindow = _outcomes.Count;
            if (totalInWindow < options.MinimumThroughput)
                return;

            var failureRate = (double)_failuresInWindow / totalInWindow;
            if (failureRate > options.FailureThreshold)
            {
                _openUntilUtc = nowUtc + options.BreakDuration;
                _outcomes.Clear();
                _failuresInWindow = 0;
            }
        }
    }

    public InvalidOperationException CreateOpenCircuitException(string consumerName) =>
        new(
            $"Circuit breaker is temporarily open for consumer {consumerName}. " +
            $"Dispatch will resume after {options.BreakDuration}.");

    private void PruneWindow(DateTimeOffset nowUtc)
    {
        var oldestAllowed = nowUtc - options.SamplingDuration;
        while (_outcomes.TryPeek(out var oldest) && oldest.TimestampUtc < oldestAllowed)
        {
            var removed = _outcomes.Dequeue();
            if (removed.IsFailure)
                _failuresInWindow--;
        }
    }

    private readonly record struct CircuitBreakerOutcome(DateTimeOffset TimestampUtc, bool IsFailure);
}
