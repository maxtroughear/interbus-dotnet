using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using InterBus.Transporting;

namespace InterBus.Tests;

public sealed class FifoConfigurationTests
{
    [Fact]
    public void ConfigureFifo_AddsTypeToFifoMessageTypes()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus => { bus.Configure<FifoTestOrderPlaced>(c => c.Fifo(o => o.CustomerId)); });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();

        var orderPlacedDescriptor = options.GetPublisherDescriptor<FifoTestOrderPlaced>();
        var orderShippedDescriptor = options.GetPublisherDescriptor<FifoTestOrderShipped>();

        Assert.True(orderPlacedDescriptor.IsFifo);
        Assert.False(orderShippedDescriptor.IsFifo);
    }

    [Fact]
    public void ConfigureGroupBy_DoesNotMarkFifo()
    {
        var services = new ServiceCollection();
        services.AddInterBus(bus => { bus.Configure<FifoTestOrderPlaced>(c => c.GroupBy(o => o.CustomerId)); });

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<InterBusOptions>();

        var orderPlacedDescriptor = options.GetPublisherDescriptor<FifoTestOrderPlaced>();

        Assert.False(orderPlacedDescriptor.IsFifo);
    }

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

        var orderPlacedDescriptor = options.GetPublisherDescriptor<FifoTestOrderPlaced>();

        Assert.False(orderPlacedDescriptor.IsFifo);
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

        var orderPlacedDescriptor = options.GetPublisherDescriptor<FifoTestOrderPlaced>();

        Assert.False(orderPlacedDescriptor.IsFifo);
    }

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
