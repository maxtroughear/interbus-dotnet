namespace InterBus.Tests.InMemory;

public partial class RuntimeConcurrencyTests
{
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var active = Interlocked.Increment(ref gate.Active);
            int previous;
            do
            {
                previous = Volatile.Read(ref gate.Maximum);
                if (previous >= active)
                    break;
            }
            while (Interlocked.CompareExchange(ref gate.Maximum, active, previous) != previous);
            if (active == gate.Expected)
                gate.Entered.TrySetResult();
            try
            {
                await gate.Release.Task.WaitAsync(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 6)
                    gate.Done.TrySetResult();
            }
        }
    }
}
