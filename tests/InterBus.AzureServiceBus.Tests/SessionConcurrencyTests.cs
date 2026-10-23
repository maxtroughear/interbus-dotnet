using InterBus.Generated;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class SessionConcurrencyTests() : ServiceBusFixture(bus =>
{
    bus.Services.AddSingleton<Gate>();
    bus.Configure<SessionOrderPlaced>(options => options.Fifo(message => message.CustomerId));
    bus.AddConsumer<BlockingSessionConsumer>(consumer => consumer.WithFifo().WithConcurrency(2));
})
{
    protected override bool IsolateEntities => true;

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
                    await publisher.PublishAsync(new SessionOrderPlaced(Guid.NewGuid(), group, sequence), TestContext.CurrentContext.CancellationToken);
            await gate.BothEntered.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.CurrentContext.CancellationToken);
            Assert.That(gate.Active, Is.EqualTo(2));
            Assert.That(gate.Started, Has.Count.EqualTo(2));
            Assert.That(gate.Started.All(item => item.Sequence == 0), Is.True);
        }
        finally { gate.Release.TrySetResult(); }
        await gate.Completed.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.CurrentContext.CancellationToken);
        foreach (var group in new[] { "a", "b" })
            Assert.That(gate.Started.Where(item => item.Group == group).Select(item => item.Sequence), Is.EqualTo(new[] { 0, 1 }));
        Assert.That(gate.Maximum, Is.EqualTo(2));
        Assert.That(gate.PerSessionMaximum.Values, Is.All.EqualTo(1));
    }

    [Test]
    public async Task RepeatedPauseResume_WithRealSessionProcessor_PreservesOrder()
    {
        var gate = Provider.GetRequiredService<Gate>();
        gate.Release.SetResult();
        var pump = Provider.GetRequiredService<PumpController>().Pumps.Single();
        await using var scope = Provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        for (var i = 0; i < 3; i++)
        {
            await pump.PauseAsync(TestContext.CurrentContext.CancellationToken).WaitAsync(TimeSpan.FromSeconds(30), TestContext.CurrentContext.CancellationToken);
            await publisher.PublishAsync(new SessionOrderPlaced(Guid.NewGuid(), "same-session", i), TestContext.CurrentContext.CancellationToken);
            Assert.That(gate.Started.Count, Is.EqualTo(i));
            await pump.ResumeAsync(TestContext.CurrentContext.CancellationToken);
            await pump.ResumeAsync(TestContext.CurrentContext.CancellationToken);
            await WaitUntilAsync(() => gate.Started.Count == i + 1, timeoutMs: 30_000,
                cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(gate.Started.Count, Is.EqualTo(i + 1));
        }
        await pump.PauseAsync(TestContext.CurrentContext.CancellationToken);
        Assert.That(gate.Started.Select(item => item.Sequence), Is.EqualTo(new[] { 0, 1, 2 }));
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
            var inSession = gate.PerSessionActive.AddOrUpdate(message.CustomerId, 1, (_, count) => count + 1);
            gate.PerSessionMaximum.AddOrUpdate(message.CustomerId, inSession, (_, maximum) => Math.Max(maximum, inSession));
            // Publish the entry before Active can signal that both sessions are ready.
            gate.Started.Enqueue((message.CustomerId, (int)message.Total));
            var active = Interlocked.Increment(ref gate.Active);
            int previous;
            do { previous = Volatile.Read(ref gate.Maximum); if (previous >= active) break; }
            while (Interlocked.CompareExchange(ref gate.Maximum, active, previous) != previous);
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
