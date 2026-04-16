using InterBus.Transport;

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
    /// Send a message to the destination described by <paramref name="context"/>.
    /// The URI scheme of <see cref="TransportSendContext.Destination"/> determines whether the
    /// message is fan-out published to a topic or sent point-to-point to a named queue, as well
    /// as which transport handles delivery.
    /// </summary>
    /// <param name="context">
    /// Carries all data required for the send: destination URI, serialised payload, headers,
    /// and an optional <see cref="TransportSendContext.Delay"/> before the message becomes
    /// visible to consumers.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SendAsync(TransportSendContext context, CancellationToken cancellationToken);
}
