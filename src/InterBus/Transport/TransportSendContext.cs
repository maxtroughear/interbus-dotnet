using InterBus.Send;

namespace InterBus.Transport;

/// <summary>
/// Carries all the data required for a single transport send operation. Passed to
/// <see cref="IInterBusTransport.SendAsync"/> so that all relevant properties are available to the transport
/// implementation
/// </summary>
public sealed class TransportSendContext(Uri destination, byte[] payload, MessageHeaders headers, TimeSpan? delay)
    : SendContext(destination, payload, headers, delay);
