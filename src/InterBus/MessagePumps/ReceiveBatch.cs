namespace InterBus.MessagePumps;

/// <summary>Iteration-scoped ownership reservations. Disposal returns only slots not transferred to the core.</summary>
public sealed class ReceiveBatch : IDisposable
{
    private readonly List<DeliverySink.Reservation> _reservations;
    private int _used;
    private bool _disposed;
    internal ReceiveBatch(List<DeliverySink.Reservation> reservations) => _reservations = reservations;
    /// <summary>Maximum deliveries this iteration may acquire.</summary>
    public int Capacity => _reservations.Count;
    /// <summary>Transfers ownership immediately. On failure the caller still owns the delivery.</summary>
    /// <param name="delivery">Acquired delivery with renewal already running if required by its transport.</param>
    /// <exception cref="ObjectDisposedException">The receive iteration has ended.</exception>
    /// <exception cref="InvalidOperationException">All reserved ownership slots have been transferred.</exception>
    public void Transfer(IDelivery delivery)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_used == Capacity) throw new InvalidOperationException("Receive batch capacity exceeded");
        _reservations[_used].Handoff(delivery);
        _used++;
    }
    /// <summary>Ends the iteration and returns unused capacity.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var reservation in _reservations) reservation.Dispose();
    }
}
