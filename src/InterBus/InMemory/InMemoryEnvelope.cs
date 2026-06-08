namespace InterBus.InMemory;

/// <summary>
/// Pairs a serialised message payload with its headers for in-process transport.
/// The <see cref="InterBusHeaders.MessageType"/> header embedded in <see cref="Headers"/> is used
/// by the consumer's dispatch delegate to route to the correct <c>Consume</c> overload.
/// </summary>
internal sealed record InMemoryEnvelope(byte[] Payload, MessageHeaders Headers, uint DeliveryCount = 0)
{
    public uint DeliveryCount { get; set; } = DeliveryCount;
}
