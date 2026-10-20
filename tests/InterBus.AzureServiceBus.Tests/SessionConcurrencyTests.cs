using System.Collections.Concurrent;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class SessionConcurrencyTests() : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<Gate>();
    bus.Configure<SessionOrderPlaced>(options => options.Fifo(message => message.CustomerId));
    bus.AddConsumer<BlockingSessionConsumer>(consumer => consumer.WithFifo().WithConcurrency(2));
})
{
    [Test]
    public async Task IndependentSessions_Overlap_WhileEachSessionRemainsOrdered()
    {
        var gate = Provider.GetRequiredService<Gate>();
        try
        {
            await using var scope = Provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            foreach (var group in new[] { "a", "b" })
                for (var sequence = 0; sequence < 2; sequence++)
                    await publisher.PublishAsync(new SessionOrderPlaced(Guid.NewGuid(), group, sequence));
            await gate.BothEntered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.That(gate.Active, Is.EqualTo(2));
            Assert.That(gate.Started, Has.Count.EqualTo(2));
            Assert.That(gate.Started.All(item => item.Sequence == 0), Is.True);
        }
        finally { gate.Release.TrySetResult(); }
        await gate.Completed.Task.WaitAsync(TimeSpan.FromSeconds(30));
        foreach (var group in new[] { "a", "b" })
            Assert.That(gate.Started.Where(item => item.Group == group).Select(item => item.Sequence), Is.EqualTo(new[] { 0, 1 }));
        Assert.That(gate.Maximum, Is.EqualTo(2));
        Assert.That(gate.PerSessionMaximum.Values, Is.All.EqualTo(1));
    }

    public sealed class Gate
    {
        public int Active, Maximum, Finished;
        public ConcurrentQueue<(string Group, int Sequence)> Started { get; } = new();
        public ConcurrentDictionary<string, int> PerSessionActive { get; } = new();
        public ConcurrentDictionary<string, int> PerSessionMaximum { get; } = new();
        public TaskCompletionSource BothEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class BlockingSessionConsumer(Gate gate) : IConsumer<SessionOrderPlaced>
    {
        public async Task Consume(SessionOrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var active = Interlocked.Increment(ref gate.Active);
            int previous;
            do { previous = Volatile.Read(ref gate.Maximum); if (previous >= active) break; }
            while (Interlocked.CompareExchange(ref gate.Maximum, active, previous) != previous);
            var inSession = gate.PerSessionActive.AddOrUpdate(message.CustomerId, 1, (_, count) => count + 1);
            gate.PerSessionMaximum.AddOrUpdate(message.CustomerId, inSession, (_, maximum) => Math.Max(maximum, inSession));
            gate.Started.Enqueue((message.CustomerId, (int)message.Total));
            if (active == 2) gate.BothEntered.TrySetResult();
            try { await gate.Release.Task.WaitAsync(cancellationToken); }
            finally
            {
                gate.PerSessionActive.AddOrUpdate(message.CustomerId, 0, (_, count) => count - 1);
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 4) gate.Completed.TrySetResult();
            }
        }
    }
}
