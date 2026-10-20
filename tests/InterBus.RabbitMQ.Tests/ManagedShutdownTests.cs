using InterBus.Extensions;
using InterBus.Generated;
using InterBus.MessagePumps;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.RabbitMq;

namespace InterBus.RabbitMQ.Tests;

[TestFixture]
public class ManagedShutdownTests
{
    [Test]
    public async Task RealHost_DrainsCallbacksBeforeClosingChannel()
    {
        await using var container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
        await container.StartAsync();
        var gate = new Gate();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddRabbitMqTransport(options => options.Uri = container.GetConnectionString());
            bus.AddConsumer<BlockingConsumer>(consumer => consumer.WithConcurrency(2));
        });
        using var host = builder.Build();
        await host.StartAsync();
        await using var scope = host.Services.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        for (var i = 0; i < 6; i++)
            await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "shutdown@test.com", i));
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var stop = host.StopAsync();
        try
        {
            Assert.That(stop.IsCompleted, Is.False);
            Assert.That(gate.Active, Is.EqualTo(2));
        }
        finally { gate.Release.TrySetResult(); }
        await stop.WaitAsync(TimeSpan.FromSeconds(10));
        var pump = host.Services.GetRequiredService<PumpController>().Pumps.Single();
        Assert.That(pump.State, Is.EqualTo(PumpState.Stopped));
        Assert.That(pump.Failure, Is.Null);
        Assert.That(gate.Active, Is.Zero);
    }

    public sealed class Gate
    {
        public int Active;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref gate.Active) == 2) gate.Entered.TrySetResult();
            try { await gate.Release.Task.WaitAsync(cancellationToken); }
            finally { Interlocked.Decrement(ref gate.Active); }
        }
    }
}
