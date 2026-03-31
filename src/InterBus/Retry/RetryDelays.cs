namespace InterBus.Retry;

/// <summary>
/// Built-in delay strategies for use with <see cref="RetryOptions.DelayProvider"/>
/// </summary>
public static class RetryDelays
{
    /// <summary>
    /// No delay between retries. This is the default
    /// </summary>
    public static readonly Func<int, TimeSpan> None = _ => TimeSpan.Zero;

    /// <summary>
    /// Returns a factory that produces a constant delay regardless of attempt number
    /// </summary>
    /// <param name="delay">The fixed delay to apply before each retry</param>
    public static Func<int, TimeSpan> Constant(TimeSpan delay) => _ => delay;

    /// <summary>
    /// Returns a factory that uses exponential back-off: <c>baseDelay * 2^(attempt-1)</c>.
    /// </summary>
    /// <param name="baseDelay">The delay applied before the first retry. Each subsequent
    /// retry doubles the previous delay</param>
    /// <param name="maxDelay">
    /// An optional upper bound on the computed delay. When <c>null</c> (the default) no
    /// cap is applied
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

    /// <summary>
    /// Returns a factory that produces a linear delay: <c>baseDelay * attempt</c>
    /// </summary>
    /// <param name="baseDelay">
    /// The delay applied before the first retry. Each subsequent retry is the same as the previous delay plus the base delay
    /// </param>
    public static Func<int, TimeSpan> Linear(TimeSpan baseDelay) => attempt => baseDelay * attempt;

    /// <summary>
    /// Returns a factory that produces a delay based on the specified intervals
    /// </summary>
    /// <param name="intervals">
    /// The intervals to use for each retry. The maximum number of retries is still governed by the
    /// <see cref="RetryOptions.RetryCount"/> property, but if the retry count exceeds the number of intervals provided,
    /// the last interval will be used for all further retries
    /// </param>
    public static Func<int, TimeSpan> Intervals(TimeSpan[] intervals) =>
        attempt => intervals[Math.Min(attempt - 1, intervals.Length - 1)];
}
