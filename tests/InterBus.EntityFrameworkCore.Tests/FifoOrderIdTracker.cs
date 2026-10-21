namespace InterBus.EntityFrameworkCore.Tests;

/// <summary>Tracker that preserves insertion order (needed for FIFO assertions).</summary>
public sealed class FifoOrderIdTracker
{
    private readonly object _lock = new();
    private readonly List<Guid> _orderIds = [];
    public IReadOnlyList<Guid> OrderIds
    {
        get
        {
            lock (_lock)
                return [.. _orderIds];
        }
    }

    public int Count
    {
        get
        {
            lock (_lock)
                return _orderIds.Count;
        }
    }

    public void Track(Guid orderId)
    {
        lock (_lock)
            _orderIds.Add(orderId);
    }
}
