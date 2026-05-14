using InterBus.Publishing;

namespace InterBus;

/// <summary>
/// Sends a request message and waits for a reply (RPC-like semantics).
/// The client sets the <see cref="InterBusHeaders.ReplyTo"/> header on the outgoing request so
/// that the consumer knows where to send the response. Resolves as a scoped service.
/// </summary>
/// <typeparam name="TRequest">The request message type.</typeparam>
/// <typeparam name="TReply">The reply message type.</typeparam>
public interface IRequestClient<TRequest, TReply>
    where TRequest : class
    where TReply : class
{
    /// <summary>
    /// Sends <paramref name="request"/> and waits for a reply of type <typeparamref name="TReply"/>.
    /// </summary>
    Task<TReply> RequestAsync(TRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends <paramref name="request"/> with additional header/delay configuration and waits for
    /// a reply of type <typeparamref name="TReply"/>.
    /// </summary>
    Task<TReply> RequestAsync(TRequest request, Action<PublishContext<TRequest>> configure,
        CancellationToken cancellationToken = default);
}
