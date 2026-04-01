namespace InterBus;

/// <summary>
/// Internal contract implemented by each transport. Used by the core <see cref="IMessagePublisher"/>
/// implementation to dispatch serialised messages without knowing the physical transport details.
/// Transport packages register their implementation of this interface via DI.
/// <para>
/// The <paramref name="destination"/> URI encodes both the transport type and the destination
/// (topic or queue) in a single value. The URI scheme identifies the transport and destination
/// type (e.g. <c>memorytopic://</c> for an in-memory topic fan-out,
/// <c>memoryqueue://</c> for a direct in-memory queue send). The host is always
/// <c>interbus</c>. The path is the destination name (topic or queue name).
/// </para>
/// </summary>
public interface IInterBusTransport
{
    /// <summary>
    /// Send a serialised payload to the destination identified by <paramref name="destination"/>.
    /// The URI scheme determines whether the message is fan-out published to a topic or sent
    /// point-to-point to a named queue, as well as which transport handles delivery.
    /// </summary>
    /// <param name="destination">
    /// A URI whose scheme encodes the transport and destination type
    /// (e.g. <c>memorytopic://interbus/my-topic</c>), host is <c>interbus</c>,
    /// and path is the destination name.
    /// </param>
    /// <param name="payload">The serialised message bytes.</param>
    /// <param name="headers">The message headers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SendAsync(Uri destination, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken);
}
