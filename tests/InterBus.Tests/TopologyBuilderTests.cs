using InterBus.Dispatch;
using InterBus.Extensions;
using InterBus.Transport;
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
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IInterBusTopologyBuilder>(topologyBuilder);
            bus.AddConsumer<TestConsumer>(
                [typeof(TestMessage)],
                static (DispatchContext _, IServiceProvider _, CancellationToken _) => Task.CompletedTask);
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
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IInterBusTopologyBuilder>(topologyBuilder);
        });

        await using var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        foreach (var hostedService in hostedServices)
            await hostedService.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, topologyBuilder.CallCount);
    }

    private sealed class CapturingTopologyBuilder : IInterBusTopologyBuilder
    {
        public int CallCount { get; private set; }

        public Task BuildTopologyAsync(CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class NoOpTransport : IInterBusTransport
    {
        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed record TestMessage;

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }
}
