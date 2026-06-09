namespace InterBus.DeadLettering;

/// <summary>
/// Wraps a dead-lettered message of type <typeparamref name="T"/> together with details
/// about the failure that caused it to be dead-lettered.
/// </summary>
/// <remarks>
/// Register an <see cref="IConsumer{T}"/> where <c>T</c> is <c>DeadLetter&lt;TOriginal&gt;</c>
/// to receive dead-lettered messages of type <typeparamref name="T"/> from the application.
/// </remarks>
/// <typeparam name="T">The original message type that failed processing</typeparam>
public sealed class DeadLetter<T> where T : class
{
    /// <summary>
    /// The original message that could not be processed
    /// </summary>
    public required T OriginalMessage { get; init; }

    /// <summary>
    /// The original headers that were attached to the original message
    /// </summary>
    public required MessageHeaders OriginalHeaders { get; init; }

    /// <summary>
    /// Human-readable reason for dead-lettering
    /// </summary>
    public required string Reason { get; init; }

    /// <summary>
    /// String representation of the error or exception that caused dead-lettering
    /// </summary>
    public required string Error { get; init; }

    /// <summary>
    /// When the message was dead-lettered
    /// </summary>
    public required DateTimeOffset DeadLetteredAtUtc { get; init; }

    /// <summary>
    /// Structured, serialisable representation of the exception that caused dead-lettering,
    /// or <see langword="null"/> when the dead letter was not triggered by an exception.
    /// </summary>
    public SerializedException? Exception { get; init; }
}
