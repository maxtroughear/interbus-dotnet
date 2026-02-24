namespace InterBus.InMemory;

/// <summary>
/// Pairs a serialised message payload with its headers for in-process transport.
/// </summary>
internal sealed record InMemoryEnvelope(byte[] Payload, MessageHeaders Headers);
