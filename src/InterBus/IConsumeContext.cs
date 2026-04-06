namespace InterBus;

/// <summary>
/// Provides access to the metadata associated with the message currently being consumed,
/// and enables publishing and scheduling messages from within a consumer.
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

    /// <summary>
    /// The message publisher for publishing or sending messages from within the consumer.
    /// Resolved from the same DI scope as the consumer.
    /// </summary>
    IMessagePublisher Publisher { get; }

    /// <summary>
    /// The message scheduler for scheduling messages for deferred delivery from within the consumer.
    /// Resolved from the same DI scope as the consumer.
    /// Returns <c>null</c> when scheduling has not been configured
    /// (i.e. <see cref="IMessageScheduler"/> is not registered in DI).
    /// </summary>
    IMessageScheduler? Scheduler { get; }
}
