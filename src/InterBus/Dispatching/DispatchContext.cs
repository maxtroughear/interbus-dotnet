using System.Diagnostics;
using InterBus.Consuming;
using InterBus.Middleware;

namespace InterBus.Dispatching;

public sealed class DispatchContext(ConsumerDescriptor descriptor, byte[] payload, MessageHeaders headers) : IContext
{
    public ConsumerDescriptor Descriptor { get; } = descriptor;

    /// <summary>
    /// The raw message payload. Dispatch middleware may replace this value, for example
    /// to retrieve the original body from external storage (Claim Check).
    /// </summary>
    public byte[] Payload { get; set; } = payload;

    public MessageHeaders Headers { get; } = headers;

    /// <summary>
    /// W3C trace parent context extracted from the message headers by the message pump.
    /// When set, <see cref="Telemetry.TelemetryDispatchMiddleware"/> uses this as the parent
    /// for the consumer activity instead of re-parsing the headers inside the middleware.
    /// </summary>
    public ActivityContext? RemoteParentContext { get; init; }
}
