namespace InterBus.Retry;

/// <summary>
/// Built-in delay strategies for use with <see cref="RetryOptions.DelayProvider"/>.
/// </summary>
public static class RetryDelays
{
    /// <summary>
    /// No delay between retries. This is the default.
    /// </summary>
    public static readonly Func<int, TimeSpan> None = _ => TimeSpan.Zero;

    /// <summary>
    /// Returns a factory that produces a constant delay regardless of attempt number.
    /// </summary>
    /// <param name="delay">The fixed delay to apply before each retry.</param>
    public static Func<int, TimeSpan> Constant(TimeSpan delay) => _ => delay;

    /// <summary>
    /// Returns a factory that uses exponential back-off: <c>baseDelay * 2^(attempt-1)</c>.
    /// </summary>
    /// <param name="baseDelay">The delay applied before the first retry. Each subsequent
    /// retry doubles the previous delay.</param>
    /// <param name="maxDelay">
    /// An optional upper bound on the computed delay. When <c>null</c> (the default) no
    /// cap is applied.
    /// </param>
    public static Func<int, TimeSpan> Exponential(TimeSpan baseDelay, TimeSpan? maxDelay = null)
    {
        return attempt =>
        {
            // Use long arithmetic to avoid overflow for large attempt numbers.
            var multiplier = attempt <= 30 ? 1L << (attempt - 1) : long.MaxValue;
            var ticks = baseDelay.Ticks >= 0
                ? Math.Min(baseDelay.Ticks, long.MaxValue / Math.Max(multiplier, 1)) * multiplier
                : 0L;
            var delay = TimeSpan.FromTicks(ticks);
            return maxDelay.HasValue && delay > maxDelay.Value ? maxDelay.Value : delay;
        };
    }
}
