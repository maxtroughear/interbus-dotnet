namespace InterBus;

/// <summary>
/// Well-known header keys set by InterBus internally.
/// All InterBus-managed headers are prefixed with <c>interbus-</c> to avoid collisions
/// with user-defined headers.
/// </summary>
public static class InterBusHeaders
{
    /// <summary>Header key for the correlation identifier.</summary>
    public const string CorrelationId = "interbus-correlation-id";

    /// <summary>
    /// Header key for the fully-qualified message type name (e.g. <c>MyApp.OrderPlaced</c>).
    /// Set automatically by <see cref="IPublisher"/> on every published and sent message.
    /// Source-generated consumer dispatch delegates read this header to select the correct
    /// <c>Consume</c> overload without runtime reflection.
    /// </summary>
    public const string MessageType = "interbus-message-type";

    /// <summary>
    /// Header key for the message identifier. Set automatically by <see cref="IPublisher"/>
    /// on every published and sent message
    /// </summary>
    public const string MessageId = "interbus-message-id";

    /// <summary>
    /// Header key for the scheduled delivery time. Set by InterBus when a message is scheduled for future delivery,
    /// and read by InterBus when the message is delivered to determine if the message should be processed now or
    /// redelivered to the queue for later processing. The value is a UTC timestamp in ISO 8601 format
    /// (e.g. <c>2024-06-01T12:00:00Z</c>).
    /// </summary>
    /// <remarks>
    /// The value is a UTC timestamp in ISO 8601 format (e.g. <c>2024-06-01T12:00:00Z</c>)
    /// </remarks>
    public const string ScheduledDeliveryTime = "interbus-scheduled-delivery-time";

    /// <summary>
    /// Header key for the time the message was sent. Set automatically by <see cref="IPublisher"/> on every published and sent message.
    /// </summary>
    /// <remarks>
    /// The value is a UTC timestamp in ISO 8601 format (e.g. <c>2024-06-01T12:00:00Z</c>)
    /// </remarks>
    public const string SentTime = "interbus-sent-time";

    /// <summary>
    /// Header key indicating the expiration time of a message.
    /// Specifies the time limit after which the message will no longer be considered valid.
    /// </summary>
    /// <remarks>
    /// The value is a UTC timestamp in ISO 8601 format (e.g. <c>2024-06-01T12:00:00Z</c>)
    /// </remarks>
    public const string ExpirationTime = "interbus-expiration-time";

    /// <summary>
    /// Header key for the causation identifier.
    /// Set automatically by <see cref="IConsumeContext"/> on outgoing messages created from within
    /// a consumer's <c>Consume</c> method, including publish, send, and schedule operations.
    /// When available, the value is set to the <see cref="MessageId"/> of the message that
    /// triggered the current consume operation. Enables causal tracing across related messages.
    /// </summary>
    public const string CausationId = "interbus-causation-id";

    /// <summary>
    /// Header key describing why a message was dead-lettered.
    /// </summary>
    public const string DeadLetterReason = "interbus-dead-letter-reason";

    /// <summary>
    /// Header key containing details of the exception that caused dead-lettering.
    /// </summary>
    public const string DeadLetterError = "interbus-dead-letter-error";

    /// <summary>
    /// Header key storing the UTC time when the message was dead-lettered.
    /// </summary>
    public const string DeadLetteredAt = "interbus-dead-lettered-at";
}
