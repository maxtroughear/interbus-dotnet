using System.Collections.Concurrent;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class OrderPlacedPublishingTracker
{
    private readonly ConcurrentBag<Guid> _orderIds = [];
    public IReadOnlyCollection<Guid> OrderIds => _orderIds;

    public void Track(Guid orderId) => _orderIds.Add(orderId);
}
