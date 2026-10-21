using InterBus.Generated;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public partial class RuntimeConcurrencyTests
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
}
