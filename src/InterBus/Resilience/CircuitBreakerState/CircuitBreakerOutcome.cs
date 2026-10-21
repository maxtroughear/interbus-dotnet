namespace InterBus.Resilience;

internal sealed partial class CircuitBreakerState
{
    private readonly record struct CircuitBreakerOutcome(DateTimeOffset TimestampUtc, bool IsFailure);
}
