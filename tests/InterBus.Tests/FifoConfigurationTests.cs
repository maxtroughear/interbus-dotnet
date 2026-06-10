using InterBus.Extensions;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

// Types used only by FifoConfigurationTests, defined at namespace level to avoid
// source-generator accessibility issues with nested private types.
public record FifoTestOrderPlaced(Guid OrderId, string CustomerId);
public record FifoTestOrderShipped(Guid OrderId);

public sealed class FifoConfigurationTests
{

    // ── MessageGroupIdRegistry ──────────────────────────────────────────────

    [Fact]
    public void Registry_NoSelector_ReturnsNull()
    {
        var registry = new MessageGroupIdRegistry();
        var result = registry.TryResolve(new FifoTestOrderPlaced(Guid.NewGuid(), "cust-1"));
        Assert.Null(result);
    }

    [Fact]
    public void Registry_WithSelector_ReturnsValue()
    {
        var registry = new MessageGroupIdRegistry();
        registry.Register<FifoTestOrderPlaced>(o => o.CustomerId);

        var result = registry.TryResolve(new FifoTestOrderPlaced(Guid.NewGuid(), "cust-42"));
        Assert.Equal("cust-42", result);
    }

    [Fact]
    public void Registry_Register_ReplacesExistingSelector()
    {
        var registry = new MessageGroupIdRegistry();
        registry.Register<FifoTestOrderPlaced>(_ => "first");
        registry.Register<FifoTestOrderPlaced>(_ => "second");

        var result = registry.TryResolve(new FifoTestOrderPlaced(Guid.NewGuid(), "x"));
        Assert.Equal("second", result);
    }

    // ── Configure<T>().Fifo() populates FifoMessageTypes ──────────────────

    [Fact]
    public void ConfigureFifo_AddsTypeToFifoMessageTypes()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.Configure<FifoTestOrderPlaced>(c => c.Fifo(o => o.CustomerId));
        });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();

        Assert.True(options.IsFifoMessageType(typeof(FifoTestOrderPlaced)));
        Assert.False(options.IsFifoMessageType(typeof(FifoTestOrderShipped)));
    }

    [Fact]
    public void ConfigureFifo_WithoutSelector_MarksFifoWithoutRegistryEntry()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.Configure<FifoTestOrderPlaced>(c => c.Fifo());
        });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();
        var registry = sp.GetRequiredService<MessageGroupIdRegistry>();

        Assert.True(options.IsFifoMessageType(typeof(FifoTestOrderPlaced)));
        // No selector registered → TryResolve returns null (falls back to CorrelationId at runtime)
        Assert.Null(registry.TryResolve(new FifoTestOrderPlaced(Guid.NewGuid(), "c1")));
    }

    [Fact]
    public void ConfigureGroupBy_DoesNotMarkFifo()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.Configure<FifoTestOrderPlaced>(c => c.GroupBy(o => o.CustomerId));
        });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();

        Assert.False(options.IsFifoMessageType(typeof(FifoTestOrderPlaced)));
    }

    // ── WithFifo() does not implicitly mark message types as FIFO ──────────

    [Fact]
    public void WithFifo_ConsumerMessageTypesRemainNonFifoWithoutConfigureFifo()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.RegisterConsumer<FifoOrderConsumer>(c => c
                .WithFifo()
                .Consumes<FifoOrderConsumer, FifoTestOrderPlaced>());
        });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();

        Assert.False(options.IsFifoMessageType(typeof(FifoTestOrderPlaced)));
    }

    [Fact]
    public void WithoutFifo_ConsumerMessageTypesNotFifo()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.RegisterConsumer<FifoOrderConsumer>(c => c
                .Consumes<FifoOrderConsumer, FifoTestOrderPlaced>());
        });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();

        Assert.False(options.IsFifoMessageType(typeof(FifoTestOrderPlaced)));
    }

    // ── Publisher stamps MessageGroupId header ──────────────────────────────

    [Fact]
    public async Task Publish_WithGroupBySelector_StampsMessageGroupIdHeader()
    {
        var capturer = new HeaderCaptureTransport();
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.Configure<FifoTestOrderPlaced>(c => c.GroupBy(o => o.CustomerId));
            bus.Options.SetTransportUriSchemes("topic", "queue");
            bus.Services.AddKeyedSingleton<ITransport>("topic", capturer);
        });

        await using var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new FifoTestOrderPlaced(Guid.NewGuid(), "cust-99");
        await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

        Assert.Equal("cust-99", capturer.LastHeaders?[InterBusHeaders.MessageGroupId]);
    }

    [Fact]
    public async Task Publish_NoGroupBySelector_FallsBackToCorrelationId()
    {
        var capturer = new HeaderCaptureTransport();
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.Options.SetTransportUriSchemes("topic", "queue");
            bus.Services.AddKeyedSingleton<ITransport>("topic", capturer);
        });

        await using var sp = services.BuildServiceProvider();
        using var scope = sp.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        var msg = new FifoTestOrderPlaced(Guid.NewGuid(), "cust-1");
        await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

        var headers = capturer.LastHeaders!;
        Assert.NotNull(headers[InterBusHeaders.MessageGroupId]);
        Assert.Equal(headers[InterBusHeaders.CorrelationId],
            headers[InterBusHeaders.MessageGroupId]);
    }

    // ── Configure<T>().TopicName() ─────────────────────────────────────────

    [Fact]
    public void Configure_TopicName_SetsTopicName()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus =>
        {
            bus.Configure<FifoTestOrderPlaced>(c => c.TopicName("orders"));
        });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();

        Assert.True(options.TryGetTopicName(typeof(FifoTestOrderPlaced), out var name));
        Assert.Equal("orders", name);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    internal sealed class FifoOrderConsumer : IConsumer<FifoTestOrderPlaced>
    {
        public Task Consume(FifoTestOrderPlaced message, IConsumeContext context,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class HeaderCaptureTransport : ITransport
    {
        public MessageHeaders? LastHeaders { get; private set; }

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            LastHeaders = context.Headers;
            return Task.CompletedTask;
        }
    }
}
