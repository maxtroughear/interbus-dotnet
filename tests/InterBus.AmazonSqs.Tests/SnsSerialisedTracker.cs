using System.Collections.Concurrent;

namespace InterBus.AmazonSqs.Tests;

public sealed class SnsSerialisedTracker
{
    private readonly ConcurrentBag<SnsSerialisedConsumedMessage> _messages = [];
    public IReadOnlyCollection<SnsSerialisedConsumedMessage> Messages => _messages;

    public void Track(OrderPlaced message, MessageHeaders headers) => _messages.Add(new SnsSerialisedConsumedMessage(message, headers));
}
