namespace InterBus;

/// <summary>
/// Represents a temporary reply endpoint created by <see cref="IReplyEndpointFactory"/>.
/// The endpoint owns the transport-level temporary queue for the duration of a single
/// request/reply exchange, and must be disposed when the exchange completes.
/// </summary>
/// <typeparam name="TReply">The expected reply message type.</typeparam>
public interface IReplyEndpoint<TReply> : IAsyncDisposable
    where TReply : class
{
    /// <summary>
    /// The address of the temporary reply queue.
    /// Set as the <see cref="InterBusHeaders.ReplyTo"/> header on the outgoing request
    /// so that the consumer knows where to send the response.
    /// </summary>
    Uri Address { get; }

    /// <summary>
    /// Waits for a reply of type <typeparamref name="TReply"/> on this endpoint.
    /// Messages that cannot be deserialized as <typeparamref name="TReply"/> are silently skipped.
    /// </summary>
    Task<TReply> ReceiveAsync(CancellationToken cancellationToken = default);
}
