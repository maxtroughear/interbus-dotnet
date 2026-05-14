namespace InterBus;

/// <summary>
/// Provides access to the metadata associated with the message currently being consumed,
/// and enables publishing and scheduling messages from within a consumer.
/// An instance is passed directly to <see cref="IConsumer{T}.Consume"/> for each message dispatch.
/// </summary>
public interface IConsumeContext
{
    /// <summary>The headers attached to the current message.</summary>
    MessageHeaders Headers { get; }

    /// <summary>
    /// Convenience accessor for the <see cref="InterBusHeaders.CorrelationId"/> header.
    /// Returns <c>null</c> if the header is not present.
    /// </summary>
    string? CorrelationId { get; }

    /// <summary>
    /// The reply-to address from the <see cref="InterBusHeaders.ReplyTo"/> header,
    /// parsed as a <see cref="Uri"/>. Returns <c>null</c> if the header is absent or
    /// cannot be parsed as a valid URI.
    /// Set on request messages by <see cref="IRequestClient{TRequest,TReply}"/>.
    /// </summary>
    Uri? ReplyTo { get; }
}
