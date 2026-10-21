namespace InterBus.AzureServiceBus.Tests;

public partial class SessionConcurrencyTests
{
    public sealed class BlockingSessionConsumer(Gate gate) : IConsumer<SessionOrderPlaced>
    {
        public async Task Consume(SessionOrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
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
            var inSession = gate.PerSessionActive.AddOrUpdate(message.CustomerId, 1, (_, count) => count + 1);
            gate.PerSessionMaximum.AddOrUpdate(message.CustomerId, inSession, (_, maximum) => Math.Max(maximum, inSession));
            gate.Started.Enqueue((message.CustomerId, (int)message.Total));
            if (active == 2)
                gate.BothEntered.TrySetResult();
            try
            {
                await gate.Release.Task.WaitAsync(cancellationToken);
            }
            finally
            {
                gate.PerSessionActive.AddOrUpdate(message.CustomerId, 0, (_, count) => count - 1);
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 4)
                    gate.Completed.TrySetResult();
            }
        }
    }
}
