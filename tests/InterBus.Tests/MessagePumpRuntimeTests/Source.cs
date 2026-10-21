using InterBus.MessagePumps;

namespace InterBus.Tests;

public partial class MessagePumpRuntimeTests
{
    private sealed class Source(int prefetch) : IBufferedSource
    {
        public int Owned;
        public int PrefetchCount => prefetch;

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public async Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken)
        {
            while (true)
            {
                using var reservation = await sink.ReserveAsync(intakeToken);
                Interlocked.Increment(ref Owned);
                var delivery = new Delivery
                {
                    Cleanup = () => Interlocked.Decrement(ref Owned)
                };
                try
                {
                    reservation.Handoff(delivery);
                }
                catch
                {
                    await delivery.DisposeAsync();
                    throw;
                }
            }
        }
    }
}
