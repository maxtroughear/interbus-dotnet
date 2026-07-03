namespace InterBus.Resilience;

/// <summary>
/// Configures dispatch circuit-breaker behaviour for <see cref="CircuitBreakerDispatchMiddleware"/>.
/// </summary>
public sealed class CircuitBreakerOptions
{
    /// <summary>
    /// The rolling window used to measure message outcomes.
    /// </summary>
    public TimeSpan SamplingDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Minimum number of outcomes required in the current sampling window before
    /// failure percentage is evaluated.
    /// </summary>
    public int MinimumThroughput { get; set; } = 20;

    /// <summary>
    /// Failure rate threshold that must be exceeded to trip the breaker.
    /// For example, <c>0.5</c> means the breaker opens only when failures are above 50%.
    /// Valid range is greater than 0 and up to 1.
    /// </summary>
    public double FailureThreshold { get; set; } = 0.5;

    /// <summary>
    /// How long the breaker remains open once tripped.
    /// </summary>
    public TimeSpan BreakDuration { get; set; } = TimeSpan.FromSeconds(30);

    internal void Validate()
    {
        if (SamplingDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SamplingDuration),
                "Sampling duration must be greater than zero");

        if (MinimumThroughput < 1)
            throw new ArgumentOutOfRangeException(nameof(MinimumThroughput),
                "Minimum throughput must be at least 1");

        if (FailureThreshold <= 0 || FailureThreshold > 1)
            throw new ArgumentOutOfRangeException(nameof(FailureThreshold),
                "Failure threshold must be greater than 0 and at most 1");

        if (BreakDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(BreakDuration),
                "Break duration must be greater than zero");
    }
}
