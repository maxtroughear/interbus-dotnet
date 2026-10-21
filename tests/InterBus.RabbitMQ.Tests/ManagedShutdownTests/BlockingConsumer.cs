namespace InterBus.RabbitMQ.Tests;

public partial class ManagedShutdownTests
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
            gate.Tokens.Enqueue(cancellationToken);
            gate.Received.AddOrUpdate(message.OrderId, 1, (_, count) => count + 1);
            if (gate.Received.Count == 6)
                gate.AllReceived.TrySetResult();
            if (active == 2)
                gate.Entered.TrySetResult();
            try
            {
                await gate.Release.Task.WaitAsync(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref gate.Active);
            }
        }
    }
}
