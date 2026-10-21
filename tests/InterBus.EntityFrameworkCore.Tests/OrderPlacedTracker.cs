using System.Collections.Concurrent;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class OrderPlacedTracker
{
    private readonly ConcurrentBag<OrderPlaced> _messages = [];
    public IReadOnlyCollection<OrderPlaced> Messages => _messages;

    public void Track(OrderPlaced msg) => _messages.Add(msg);
}
