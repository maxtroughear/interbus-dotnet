namespace InterBus.InMemory;

/// <summary>
/// Pairs a serialised message payload with its routing information and headers for in-process transport.
/// <see cref="TopicName"/> identifies the message type so the receiving queue can dispatch to the
/// correct handler when a consumer handles multiple message types.
/// </summary>
internal sealed record InMemoryEnvelope(string TopicName, string Payload, MessageHeaders Headers);
