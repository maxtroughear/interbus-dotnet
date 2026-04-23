namespace InterBus;

/// <summary>
/// Resolves the appropriate <see cref="ITransport"/> for a given destination URI.
/// <para>
/// Transports register themselves as keyed services with their supported URI scheme(s)
/// as the key (e.g. <c>"memorytopic"</c>, <c>"memoryqueue"</c>). This factory looks up
/// the registered transport by the destination scheme, enabling multiple transports to be
/// active simultaneously in the same application.
/// </para>
/// </summary>
public interface ITransportFactory
{
    /// <summary>
    /// Returns the <see cref="ITransport"/> that handles the given
    /// <paramref name="destination"/> URI.
    /// </summary>
    /// <param name="destination">
    /// The destination URI whose <see cref="Uri.Scheme"/> is used to select the transport.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no transport is registered for the URI scheme.
    /// </exception>
    ITransport Resolve(Uri destination);
}
