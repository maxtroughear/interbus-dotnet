using InterBus.Extensions;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests;

public sealed class TopologyBuilderTests
{
    [Fact]
    public async Task HostedTopologySetup_InvokesRegisteredBuilders_WhenConsumersExist()
    {
        var topologyBuilder = new CapturingTopologyBuilder();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<ITransport, NoOpTransport>();
            bus.Services.AddSingleton<ITopologyBuilder>(topologyBuilder);
            bus.RegisterConsumer<TestConsumer>(c => c.Consumes<TestConsumer, TestMessage>());
        });

        await using var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        foreach (var hostedService in hostedServices)
            await hostedService.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, topologyBuilder.CallCount);
    }

    [Fact]
    public async Task HostedTopologySetup_DoesNotInvokeBuilders_WhenNoConsumersExist()
    {
        var topologyBuilder = new CapturingTopologyBuilder();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<ITransport, NoOpTransport>();
            bus.Services.AddSingleton<ITopologyBuilder>(topologyBuilder);
        });

        await using var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        foreach (var hostedService in hostedServices)
            await hostedService.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, topologyBuilder.CallCount);
    }

    private sealed class CapturingTopologyBuilder : ITopologyBuilder
    {
        public int CallCount { get; private set; }

        public Task BuildTopologyAsync(CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class NoOpTransport : ITransport
    {
        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    internal sealed record TestMessage;

    internal sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
