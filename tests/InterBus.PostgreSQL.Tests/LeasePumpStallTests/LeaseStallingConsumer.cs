namespace InterBus.PostgreSQL.Tests;

public partial class LeasePumpStallTests
{
    public sealed class LeaseStallingConsumer(OrderPlacedTracker tracker, LeaseAtomicCounter counter) : IConsumer<OrderPlaced>
    {
        private static readonly TimeSpan StallingDuration = TimeSpan.FromSeconds(8);
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var attempt = counter.Increment();
            if (attempt == 1)
            {
                // Sleep past the lease duration. The lease expires at 3 s; we stall 8 s.
                // Use CancellationToken.None so teardown does not cut short the stall while
                // the test is still waiting on the reclaim path.
                await Task.Delay(StallingDuration, CancellationToken.None);
                // Return without tracking – ack_claim will be rejected by token mismatch.
                return;
            }

            tracker.Track(message);
        }
    }
}
