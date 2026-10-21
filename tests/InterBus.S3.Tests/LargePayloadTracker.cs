using System.Collections.Concurrent;

namespace InterBus.S3.Tests;

public sealed class LargePayloadTracker
{
    private readonly ConcurrentBag<LargePayloadMessage> _messages = [];
    public IReadOnlyCollection<LargePayloadMessage> Messages => _messages;

    public void Track(LargePayloadMessage message) => _messages.Add(message);
}
