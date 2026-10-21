namespace InterBus.PostgreSQL.Tests;

public partial class LeaseConcurrencyBoundTests
{
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var count = Interlocked.Increment(ref gate.Active);
            int previous;
            do
            {
                previous = Volatile.Read(ref gate.Maximum);
                if (previous >= count)
                    break;
            }
            while (Interlocked.CompareExchange(ref gate.Maximum, count, previous) != previous);
            if (count == 2)
                gate.Entered.TrySetResult();
            try
            {
                await gate.Release.Task.WaitAsync(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 8)
                    gate.Completed.TrySetResult();
            }
        }
    }
}
