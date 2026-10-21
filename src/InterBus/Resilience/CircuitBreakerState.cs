namespace InterBus.Resilience;

internal sealed partial class CircuitBreakerState(CircuitBreakerOptions options)
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

    public InvalidOperationException CreateOpenCircuitException(string consumerName) => new($"Circuit breaker is temporarily open for consumer {consumerName}. " + $"Dispatch will resume after {options.BreakDuration}.");
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
}
