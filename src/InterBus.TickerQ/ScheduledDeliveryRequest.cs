namespace InterBus.TickerQ;

/// <summary>
/// Serializable payload stored in TickerQ's <c>TimeTickerEntity.Request</c> field for each
/// scheduled InterBus message (both topic fan-out and point-to-point queue send).
/// The <see cref="Destination"/> URI encodes the transport type and routing semantics.
/// </summary>
public sealed class ScheduledDeliveryRequest
{
    /// <summary>
    /// The destination URI (e.g. <c>memorytopic://interbus/my-topic</c> or
    /// <c>memoryqueue://interbus/my-queue</c>). Passed directly to
    /// <see cref="IInterBusTransport.SendAsync"/> when the scheduled job fires.
    /// </summary>
    public string Destination { get; init; } = string.Empty;

    /// <summary>The serialized message payload bytes.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary>The message headers to forward to the transport on delivery.</summary>
    public Dictionary<string, string> Headers { get; init; } = [];

    /// <summary>Parameterless constructor required for JSON deserialization.</summary>
    public ScheduledDeliveryRequest()
    {
    }

    internal ScheduledDeliveryRequest(Uri destination, byte[] payload, MessageHeaders headers)
    {
        Destination = destination.ToString();
        Payload = payload;
        Headers = new Dictionary<string, string>(headers);
    }
}
