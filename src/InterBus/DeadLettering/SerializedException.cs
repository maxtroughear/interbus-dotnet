namespace InterBus.DeadLettering;

/// <summary>
/// A serialisable representation of an exception, suitable for inclusion in a
/// <see cref="DeadLetter{T}"/> message. Captures the exception type, message,
/// stack trace, and the full inner-exception chain.
/// </summary>
public sealed class SerializedException
{
    /// <summary>
    /// The fully-qualified type name of the exception (e.g. <c>System.InvalidOperationException</c>).
    /// </summary>
    public required string ExceptionType { get; init; }

    /// <summary>
    /// The exception message.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// The stack trace at the point the exception was thrown, or <see langword="null"/> if unavailable.
    /// </summary>
    public string? StackTrace { get; init; }

    /// <summary>
    /// The serialised inner exception, or <see langword="null"/> if there is none.
    /// </summary>
    public SerializedException? InnerException { get; init; }

    /// <summary>
    /// Creates a <see cref="SerializedException"/> from a live <see cref="Exception"/>,
    /// recursively capturing the inner-exception chain.
    /// </summary>
    public static SerializedException FromException(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return new SerializedException
        {
            ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
            Message = exception.Message,
            StackTrace = exception.StackTrace,
            InnerException = exception.InnerException is not null
                ? FromException(exception.InnerException)
                : null,
        };
    }
}
