namespace InterBus.Retry;

/// <summary>
/// Configures in-process message retry behaviour for <see cref="RetryConsumeMiddleware"/>.
/// </summary>
public sealed class RetryOptions
{
    private int _retryCount = 3;

    /// <summary>
    /// The number of times to retry after the initial attempt fails.
    /// Must be greater than zero. Defaults to <c>3</c>.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
    public int RetryCount
    {
        get => _retryCount;
        set
        {
            if (value < 1)
                throw new ArgumentOutOfRangeException(nameof(value), value, "RetryCount must be at least 1.");
            _retryCount = value;
        }
    }

    /// <summary>
    /// Computes the delay to wait before a specific retry attempt.
    /// The parameter is the one-based attempt number (1 = first retry, 2 = second retry, …).
    /// Return <see cref="TimeSpan.Zero"/> for no delay.
    /// Defaults to <see cref="RetryDelays.None"/> (no delay between attempts).
    /// </summary>
    public Func<int, TimeSpan> DelayProvider { get; set; } = RetryDelays.None;

    /// <summary>
    /// Determines whether a thrown exception should trigger a retry.
    /// Return <c>true</c> to retry, <c>false</c> to rethrow immediately.
    /// <see cref="OperationCanceledException"/> caused by the middleware's own
    /// <see cref="CancellationToken"/> is never retried regardless of this predicate.
    /// Defaults to <c>null</c>, meaning all non-cancellation exceptions are retried.
    /// </summary>
    public Func<Exception, bool>? ShouldRetry { get; set; }
}
