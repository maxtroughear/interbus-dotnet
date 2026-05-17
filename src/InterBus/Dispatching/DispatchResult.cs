namespace InterBus.Dispatching;

/// <summary>
/// Outcome of dispatching a single message through the InterBus consume pipeline.
/// </summary>
public sealed class DispatchResult
{
    private DispatchResult(
        DispatchResultKind kind,
        Exception? exception = null,
        string? deadLetterReason = null,
        string? deadLetterError = null,
        DateTimeOffset? deadLetteredAtUtc = null)
    {
        Kind = kind;
        Exception = exception;
        DeadLetterReason = deadLetterReason;
        DeadLetterError = deadLetterError;
        DeadLetteredAtUtc = deadLetteredAtUtc;
    }

    public static DispatchResult Success { get; } = new(DispatchResultKind.Success);

    public DispatchResultKind Kind { get; }

    public Exception? Exception { get; }

    public string? DeadLetterReason { get; }

    public string? DeadLetterError { get; }

    public DateTimeOffset? DeadLetteredAtUtc { get; }

    public static DispatchResult Release(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new DispatchResult(DispatchResultKind.Release, exception: exception);
    }

    public static DispatchResult DeadLetter(
        string reason,
        string error,
        DateTimeOffset deadLetteredAtUtc,
        Exception exception)
    {
        ArgumentNullException.ThrowIfNull(reason);
        ArgumentNullException.ThrowIfNull(error);
        ArgumentNullException.ThrowIfNull(exception);

        return new DispatchResult(
            DispatchResultKind.DeadLetter,
            exception,
            reason,
            error,
            deadLetteredAtUtc);
    }
}

public enum DispatchResultKind
{
    Success,
    Release,
    DeadLetter
}
