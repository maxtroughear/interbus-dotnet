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
    /// Set automatically by <see cref="IMessagePublisher"/> on every published and sent message.
    /// Source-generated consumer dispatch delegates read this header to select the correct
    /// <c>Consume</c> overload without runtime reflection.
    /// </summary>
    public const string MessageType = "interbus-message-type";
}
