using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus.Administration;
using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InterBus.AzureServiceBus.Tests;

[TestFixture]
public class PumpCircuitTests() : ServiceBusFixture(_ => { })
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Recovery_ReceivesOneDelivery_RenewsLock_AndResumes(bool sessions)
    {
        var gate = new CircuitGate();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(options =>
            {
                options.ConnectionString = Container.GetConnectionString();
                options.AdminConnectionString = Container.GetHttpConnectionString();
                options.QueuePrefix = sessions ? "circuit-session-" : "circuit-normal-";
                options.TopicPrefix = sessions ? "circuit-session-" : "circuit-normal-";
            });
            if (sessions) bus.Configure<CircuitMessage>(message => message.Fifo(_ => "ordered"));
            bus.AddConsumer<CircuitConsumer>(consumer =>
            {
                consumer.WithConcurrency(8).UsePumpCircuitBreaker(options =>
                {
                    options.Dependency(policy =>
                    {
                        policy.MinimumThroughput = 1;
                        policy.BreakDuration = TimeSpan.FromSeconds(2);
                    });
                    options.ProbeTimeout = TimeSpan.FromSeconds(30);
                    options.ReopenJitter = TimeSpan.Zero;
                });
                if (sessions) consumer.WithFifo();
            });
        });
        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            var pump = host.Services.GetRequiredService<PumpController>().Pumps.Single();
            var queueName = AzureServiceBusNaming.SanitizeEntityName((sessions ? "circuit-session-" : "circuit-normal-") + pump.Consumer.QueueName);
            var admin = new ServiceBusAdministrationClient(Container.GetHttpConnectionString());
            var queue = (await admin.GetQueueAsync(queueName)).Value;
            queue.LockDuration = TimeSpan.FromSeconds(5);
            await admin.UpdateQueueAsync(queue);
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            for (var i = 0; i < 12; i++) await publisher.PublishAsync(new CircuitMessage(i));
            await WaitUntilAsync(() => pump.Circuits.Any(p => p.State == "Open"), timeoutMs: 15_000);
            gate.Recover = true;
            await gate.Probe.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.That(pump.Circuits.Single().State, Is.EqualTo("HalfOpen"));
            // Hold beyond the queue lock duration to exercise standalone receiver renewal
            await Task.Delay(TimeSpan.FromSeconds(7), TestContext.CurrentContext.CancellationToken);
            Assert.That(gate.SuccessfulCalls, Is.EqualTo(1));
            Assert.That(pump.Circuits.Single().State, Is.EqualTo("HalfOpen"));
            gate.Finish.TrySetResult();
            await WaitUntilAsync(() => gate.Completed.Count == 12, timeoutMs: 30_000);
            Assert.That(gate.Completed.Values, Is.All.EqualTo(1));
            Assert.That(pump.Circuits.Single().State, Is.EqualTo("Closed"));
            if (sessions) Assert.That(gate.Order, Is.Ordered);
        }
        finally { gate.Finish.TrySetResult(); await host.StopAsync(); }
    }

    public sealed record CircuitMessage(int Sequence);
    public sealed class CircuitGate
    {
        public volatile bool Recover;
        public int SuccessfulCalls;
        public TaskCompletionSource Probe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ConcurrentDictionary<int, int> Completed = new();
        public ConcurrentQueue<int> Order = new();
    }
    public sealed class CircuitConsumer(CircuitGate gate) : IConsumer<CircuitMessage>
    {
        public async Task Consume(CircuitMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            if (!gate.Recover) throw new IOException("dependency unavailable");
            if (Interlocked.Increment(ref gate.SuccessfulCalls) == 1)
            {
                gate.Probe.TrySetResult();
                await gate.Finish.Task.WaitAsync(cancellationToken);
            }
            gate.Completed.AddOrUpdate(message.Sequence, 1, (_, count) => count + 1);
            gate.Order.Enqueue(message.Sequence);
        }
    }
}
