using InterBus.Generated;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public class RuntimeConcurrencyTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task RealHost_UsesConfiguredCoreWorkerCount(int concurrency)
    {
        var gate = new Gate(concurrency);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<BlockingConsumer>(consumer => consumer.WithConcurrency(concurrency));
        });
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            for (var i = 0; i < 6; i++)
                await publisher.PublishAsync(new OrderPlaced(Guid.NewGuid(), "test", i), TestContext.Current.CancellationToken);
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(concurrency, Volatile.Read(ref gate.Active));
            Assert.Single(host.Services.GetRequiredService<PumpController>().Pumps);
        }
        finally { gate.Release.TrySetResult(); }
        await gate.Done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(concurrency, gate.Maximum);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task SequentialRedelivery_KeepsAttemptStateAndPrecedesLaterWork()
    {
        var attempts = new Attempts();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(attempts);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<RetryingConsumer>(consumer => consumer.WithConcurrency(1).WithFifo());
        });
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var first = new OrderPlaced(Guid.NewGuid(), "first", 0);
        var second = new OrderPlaced(Guid.NewGuid(), "second", 1);
        await using var scope = host.Services.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(first, TestContext.Current.CancellationToken);
        await publisher.PublishAsync(second, TestContext.Current.CancellationToken);
        await attempts.Done.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(new[] { first.OrderId, first.OrderId, first.OrderId, second.OrderId }, attempts.Seen);
    }

    public sealed class Attempts
    {
        public List<Guid> Seen { get; } = [];
        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public sealed class RetryingConsumer(Attempts attempts) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            attempts.Seen.Add(message.OrderId);
            if (message.CustomerEmail == "first" && attempts.Seen.Count < 3)
                ((ConsumeContext)context).Result = DispatchResult.Release(new IOException("release for redelivery"));
            if (message.CustomerEmail == "second") attempts.Done.TrySetResult();
            return Task.CompletedTask;
        }
    }

    public sealed class Gate(int expected)
    {
        public int Expected => expected;
        public int Active, Maximum, Finished;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var active = Interlocked.Increment(ref gate.Active);
            int previous;
            do { previous = Volatile.Read(ref gate.Maximum); if (previous >= active) break; }
            while (Interlocked.CompareExchange(ref gate.Maximum, active, previous) != previous);
            if (active == gate.Expected) gate.Entered.TrySetResult();
            try { await gate.Release.Task.WaitAsync(cancellationToken); }
            finally
            {
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 6) gate.Done.TrySetResult();
            }
        }
    }
}
