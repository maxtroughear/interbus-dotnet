namespace InterBus.Retry;

/// <summary>
/// Configures transport redelivery retry behaviour for <see cref="RedeliveryRetryConsumeMiddleware"/>.
/// </summary>
public sealed class RedeliveryRetryOptions
{
    private int _retryCount = 3;

    /// <summary>
    /// The number of times to redeliver after the initial delivery fails.
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
    /// Computes the delay to wait before scheduling a specific redelivery attempt.
    /// The parameter is the one-based attempt number (1 = first redelivery retry, 2 = second, ...).
    /// Return <see cref="TimeSpan.Zero"/> for no delay.
    /// Defaults to <see cref="RetryDelays.None"/> (no delay between attempts).
    /// </summary>
    public Func<int, TimeSpan> DelayProvider { get; set; } = RetryDelays.None;

    /// <summary>
    /// Determines whether a thrown exception should trigger transport redelivery.
    /// Return <c>true</c> to redeliver, <c>false</c> to rethrow immediately.
    /// <see cref="OperationCanceledException"/> caused by the middleware's own
    /// <see cref="CancellationToken"/> is never redelivered regardless of this predicate.
    /// Defaults to <c>null</c>, meaning all non-cancellation exceptions are redelivered.
    /// </summary>
    public Func<Exception, bool>? ShouldRetry { get; set; }
}
