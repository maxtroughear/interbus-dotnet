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
    /// Failure percentage threshold that must be exceeded to trip the breaker.
    /// For example, <c>50</c> means the breaker opens only when failures are above 50%.
    /// Valid range is greater than 0 and up to 100.
    /// </summary>
    public double FailurePercentageThreshold { get; set; } = 50;

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

        if (FailurePercentageThreshold <= 0 || FailurePercentageThreshold > 100)
            throw new ArgumentOutOfRangeException(nameof(FailurePercentageThreshold),
                "Failure percentage threshold must be greater than 0 and at most 100");

        if (BreakDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(BreakDuration),
                "Break duration must be greater than zero");
    }
}
