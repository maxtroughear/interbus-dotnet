namespace InterBus.Dispatching;

/// <summary>
/// Outcome of dispatching a single message through the InterBus consume pipeline.
/// </summary>
public sealed class DispatchResult
{
    private DispatchResult(
        DispatchResultKind kind,
        Exception? exception = null,
        DateTimeOffset? deadLetteredAtUtc = null)
    {
        Kind = kind;
        Exception = exception;
        DeadLetteredAtUtc = deadLetteredAtUtc;
    }

    public static DispatchResult Success { get; } = new(DispatchResultKind.Success);

    public DispatchResultKind Kind { get; }

    public Exception? Exception { get; }

    public string DeadLetterReason
    {
        get
        {
            if (field is null && Exception is not null)
            {
                field = Exception.GetType().Name;
            }

            return field ?? "DefaultDispatchException";
        }
    }

    public string DeadLetterError
    {
        get
        {
            if (field is null && Exception is not null)
            {
                field = Exception.ToString();
            }

            return field ?? "DefaultDispatchException";
        }
    }

    public DateTimeOffset? DeadLetteredAtUtc { get; }

    public static DispatchResult Release(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new DispatchResult(DispatchResultKind.Release, exception: exception);
    }

    public static DispatchResult DeadLetter(
        DateTimeOffset deadLetteredAtUtc,
        Exception exception) =>
        new(
            DispatchResultKind.DeadLetter,
            exception,
            deadLetteredAtUtc);
}

public enum DispatchResultKind
{
    Success,
    Release,
    DeadLetter
}
