using System.Collections.Concurrent;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class OrderShippedTracker
{
    private readonly ConcurrentBag<OrderShipped> _messages = [];
    public IReadOnlyCollection<OrderShipped> Messages => _messages;

    public void Track(OrderShipped msg) => _messages.Add(msg);
}
