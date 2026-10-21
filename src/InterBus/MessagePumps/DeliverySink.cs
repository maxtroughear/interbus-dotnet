using System.Threading.Channels;

namespace InterBus.MessagePumps;

/// <summary>Shared per-consumer ownership budget, including receives, queued deliveries and active processing.</summary>
public sealed partial class DeliverySink
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
}
