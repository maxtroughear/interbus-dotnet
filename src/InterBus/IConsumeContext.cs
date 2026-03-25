namespace InterBus;

/// <summary>
/// Provides access to the metadata associated with the message currently being consumed.
/// An instance is passed directly to <see cref="IConsumer{T}.Consume"/> for each message dispatch.
/// </summary>
public interface IConsumeContext
{
    /// <summary>The headers attached to the current message.</summary>
    IReadOnlyDictionary<string, string> Headers { get; }

    /// <summary>
    /// Convenience accessor for the <see cref="InterBusHeaders.CorrelationId"/> header.
    /// Returns <c>null</c> if the header is not present.
    /// </summary>
    string? CorrelationId { get; }

    /// <summary>
    /// Convenience accessor for the <see cref="InterBusHeaders.MessageType"/> header.
    /// </summary>
    string MessageType { get; }
}
