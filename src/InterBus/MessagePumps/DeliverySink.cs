using System.Threading.Channels;

namespace InterBus.MessagePumps;

/// <summary>Shared per-consumer ownership budget, including receives, queued deliveries and active processing.</summary>
public sealed class DeliverySink
{
    private readonly SemaphoreSlim _capacity;
    private readonly Channel<Entry> _input = Channel.CreateUnbounded<Entry>(new UnboundedChannelOptions
    { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false });
    internal ChannelReader<Entry> Reader => _input.Reader;
    internal void Complete() => _input.Writer.TryComplete();

    internal DeliverySink(int capacity) => _capacity = new SemaphoreSlim(capacity, capacity);

    /// <summary>Reserves one ownership slot before receive. Dispose unused reservations on empty receive or failure.</summary>
    public async ValueTask<Reservation> ReserveAsync(CancellationToken cancellationToken)
    {
        await _capacity.WaitAsync(cancellationToken);
        return new Reservation(this);
    }

    /// <summary>Reserves another immediately available slot for a batch without waiting.</summary>
    public Reservation? TryReserve() => _capacity.Wait(0) ? new Reservation(this) : null;

    /// <summary>A single-use receive reservation. Disposing after handoff does not reclaim core ownership.</summary>
    public sealed class Reservation : IDisposable
    {
        private DeliverySink? _owner;
        internal Reservation(DeliverySink owner) => _owner = owner;
        /// <summary>Transfers ownership synchronously. If it throws, the source must dispose the delivery.</summary>
        public void Handoff(IDelivery delivery)
        {
            var owner = _owner ?? throw new InvalidOperationException("Reservation already used");
            if (!owner._input.Writer.TryWrite(new Entry(delivery, owner._capacity)))
                throw new InvalidOperationException("Consumer intake completed");
            _owner = null;
        }
        /// <summary>Returns an unused receive slot.</summary>
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?._capacity.Release();
    }

    internal sealed record Entry(IDelivery Delivery, SemaphoreSlim Capacity);
}
