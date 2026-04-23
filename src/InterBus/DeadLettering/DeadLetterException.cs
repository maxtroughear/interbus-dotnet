namespace InterBus.DeadLettering;

/// <summary>
/// Exception raised by dead-letter middleware when message consumption fails without being handled
/// by earlier middleware.
/// </summary>
public sealed class DeadLetterException(
    string reason,
    string error,
    DateTimeOffset deadLetteredAtUtc,
    Exception innerException) : Exception(reason, innerException)
{
    /// <summary>
    /// Human-readable reason used when dead-lettering with transport-native APIs.
    /// </summary>
    public string Reason { get; } = reason;

    /// <summary>
    /// String representation of the underlying error.
    /// </summary>
    public string Error { get; } = error;

    /// <summary>
    /// UTC timestamp when dead-lettering was requested.
    /// </summary>
    public DateTimeOffset DeadLetteredAtUtc { get; } = deadLetteredAtUtc;
}
