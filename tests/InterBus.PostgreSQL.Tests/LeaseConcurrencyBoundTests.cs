using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Lease))]
public class LeaseConcurrencyBoundTests(string imageTag, PostgreSqlMessagePumpMode pumpMode) : PostgreSqlFixtureBase(
    imageTag, pumpMode, bus =>
    {
        bus.Services.AddSingleton<Gate>();
        bus.AddConsumer<BlockingConsumer>(consumer => consumer.WithConcurrency(2).WithPumpCount(4));
    })
{
    [Test]
    public async Task SeveralReceivePumps_ShareTwoProcessingSlots()
    {
        var gate = Provider.GetRequiredService<Gate>();
        try
        {
            await using var scope = Provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            for (var i = 0; i < 8; i++)
                await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "bound@test.com", i));
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(Volatile.Read(ref gate.Active), Is.EqualTo(2));
            Assert.That(Volatile.Read(ref gate.Maximum), Is.EqualTo(2));
        }
        finally { gate.Release.TrySetResult(); }
        await gate.Completed.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(gate.Maximum, Is.EqualTo(2));
    }

    public sealed class Gate
    {
        public int Active, Maximum, Finished;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var count = Interlocked.Increment(ref gate.Active);
            int previous;
            do { previous = Volatile.Read(ref gate.Maximum); if (previous >= count) break; }
            while (Interlocked.CompareExchange(ref gate.Maximum, count, previous) != previous);
            if (count == 2) gate.Entered.TrySetResult();
            try { await gate.Release.Task.WaitAsync(cancellationToken); }
            finally
            {
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 8) gate.Completed.TrySetResult();
            }
        }
    }
}
