namespace InterBus.PostgreSQL.Tests;

public partial class TransactionalNotificationTests
{
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref gate.Active) == 3)
                gate.AllEntered.TrySetResult();
            try
            {
                await gate.Release.Task.WaitAsync(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 3)
                    gate.Completed.TrySetResult();
            }
        }
    }
}
