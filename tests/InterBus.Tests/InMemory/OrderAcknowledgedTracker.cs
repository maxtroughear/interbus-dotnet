namespace InterBus.Tests.InMemory;

public sealed class OrderAcknowledgedTracker
{
    private readonly System.Collections.Concurrent.ConcurrentBag<OrderAcknowledged> _messages = [];
    public IReadOnlyCollection<OrderAcknowledged> Messages => _messages;

    public void Track(OrderAcknowledged msg) => _messages.Add(msg);
}
