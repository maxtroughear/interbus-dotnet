using InterBus.Generated;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class PumpCircuitTests(string imageTag, PostgreSqlMessagePumpMode mode) : PostgreSqlFixtureBase(imageTag, mode, bus =>
{
    bus.Services.AddSingleton<CircuitGate>();
    bus.AddConsumer<CircuitConsumer>(consumer => consumer.UsePumpCircuitBreaker(options =>
    {
        options.Dependency(policy =>
        {
            policy.MinimumThroughput = 1;
            policy.BreakDuration = TimeSpan.FromSeconds(1);
        });
        options.ReopenJitter = TimeSpan.Zero;
    }));
})
{
    [Test]
    public async Task OpensThenProcessesOneProbe_BeforeResumingBacklog()
    {
        var gate = Provider.GetRequiredService<CircuitGate>();
        var pump = Provider.GetRequiredService<PumpController>().Pumps.Single();
        try
        {
            await using var scope = Provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            for (var i = 0; i < 6; i++) await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "circuit@test", i));
            await WaitUntilAsync(() => pump.Circuits.Any(p => p.State == "Open"), timeoutMs: 10_000);
            await gate.Probe.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(gate.Calls, Is.EqualTo(2));
            Assert.That(pump.Circuits.Single().State, Is.EqualTo("HalfOpen"));
            gate.Finish.TrySetResult();
            await WaitUntilAsync(() => gate.Calls == 7 && pump.Circuits.All(p => p.State == "Closed"), timeoutMs: 10_000);
        }
        finally { gate.Finish.TrySetResult(); }
    }
    public sealed class CircuitGate
    {
        public int Calls;
        public TaskCompletionSource Probe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class CircuitConsumer(CircuitGate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var call = Interlocked.Increment(ref gate.Calls);
            if (call == 1) throw new IOException("dependency unavailable");
            if (call == 2) { gate.Probe.TrySetResult(); await gate.Finish.Task.WaitAsync(cancellationToken); }
        }
    }
}
