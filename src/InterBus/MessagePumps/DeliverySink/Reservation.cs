namespace InterBus.MessagePumps;

public sealed partial class DeliverySink
{
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
}
