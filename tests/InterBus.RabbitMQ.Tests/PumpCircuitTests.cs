using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.MessagePumps;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.RabbitMq;

namespace InterBus.RabbitMQ.Tests;

[TestFixture]
public class PumpCircuitTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Backlog_UsesSingleGetWhilePaused_ThenResumes(bool recoverConnection)
    {
        await using var container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
        await container.StartAsync();
        var gate = new CircuitGate();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddRabbitMqTransport(options => options.Uri = container.GetConnectionString());
            bus.AddConsumer<CircuitConsumer>(consumer => consumer.WithConcurrency(8).UsePumpCircuitBreaker(options =>
            {
                options.Dependency(policy =>
                {
                    policy.MinimumThroughput = 1;
                    policy.BreakDuration = TimeSpan.FromSeconds(recoverConnection ? 10 : 1);
                });
                options.ReopenJitter = TimeSpan.Zero;
            }));
        });
        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            for (var i = 0; i < 20; i++) await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "circuit@test", i));
            var pump = host.Services.GetRequiredService<PumpController>().Pumps.Single();
            await WaitUntilAsync(() => pump.Circuits.Any(p => p.State == "Open"), timeoutMs: 10_000);
            await WaitUntilAsync(async () => await ConsumerCountAsync() == 0, timeoutMs: 10_000);
            if (recoverConnection)
            {
                var close = await container.ExecAsync(["rabbitmqctl", "close_all_connections", "--global", "circuit recovery"]);
                Assert.That(close.ExitCode, Is.Zero, close.Stderr);
            }
            gate.Recover = true;
            await gate.Probe.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.That(gate.SuccessfulCalls, Is.EqualTo(1));
            Assert.That(await ConsumerCountAsync(), Is.Zero);
            Assert.That(pump.Circuits.Single().State, Is.EqualTo("HalfOpen"));
            gate.Finish.TrySetResult();
            await WaitUntilAsync(() => gate.Completed.Count == 20, timeoutMs: 30_000);
            Assert.That(pump.Circuits.Single().State, Is.EqualTo("Closed"));
            Assert.That(gate.Completed.Values, Is.All.EqualTo(1));

            async Task<int> ConsumerCountAsync()
            {
                var queues = await container.ExecAsync(["rabbitmqctl", "list_queues", "--quiet", "--no-table-headers", "name", "consumers"]);
                var row = queues.Stdout.Split('\n').Select(line => line.Trim().Split('\t'))
                    .Single(parts => parts.Length == 2 && parts[0] == pump.Consumer.QueueName);
                return int.Parse(row[1]);
            }
        }
        finally { gate.Finish.TrySetResult(); await host.StopAsync(); }
    }

    public sealed class CircuitGate
    {
        public volatile bool Recover;
        public int SuccessfulCalls;
        public TaskCompletionSource Probe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ConcurrentDictionary<Guid, int> Completed = new();
    }
    public sealed class CircuitConsumer(CircuitGate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            if (!gate.Recover) throw new IOException("dependency unavailable");
            if (Interlocked.Increment(ref gate.SuccessfulCalls) == 1)
            {
                gate.Probe.TrySetResult();
                await gate.Finish.Task.WaitAsync(cancellationToken);
            }
            gate.Completed.AddOrUpdate(message.OrderId, 1, (_, count) => count + 1);
        }
    }
}
