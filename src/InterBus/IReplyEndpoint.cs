namespace InterBus;

/// <summary>
/// Represents a temporary reply endpoint created by <see cref="IReplyEndpointFactory"/>.
/// The endpoint owns the transport-level temporary queue for the duration of a single
/// request/reply exchange, and must be disposed when the exchange completes.
/// </summary>
public interface IReplyEndpoint : IAsyncDisposable
{
    /// <summary>
    /// The address of the temporary reply queue.
    /// Set as the <see cref="InterBusHeaders.ReplyTo"/> header on the outgoing request
    /// so that the consumer knows where to send the response.
    /// </summary>
    Uri Address { get; }

    /// <summary>
    /// Waits for a reply of type <typeparamref name="TReply"/> on this endpoint.
    /// The incoming message is dispatched through the full consume pipeline before being
    /// returned. Messages of a different type are silently skipped until the expected type
    /// arrives or the cancellation token fires.
    /// </summary>
    Task<TReply> ReceiveAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class;
}
