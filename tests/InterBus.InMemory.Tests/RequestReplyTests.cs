using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

public class RequestReplyTests
{
    // ── Test messages ─────────────────────────────────────────────────────────

    public record GetOrderStatus(Guid OrderId);

    public record OrderStatusReply(Guid OrderId, string Status);

    // ── Supporting types ──────────────────────────────────────────────────────

    public sealed class ReplyToCapture { public Uri? Value { get; set; } }

    public sealed class HeaderCapture2 { public string? Value { get; set; } }

    /// <summary>
    /// Consumes <see cref="GetOrderStatus"/> and sends a reply using <see cref="IConsumeContext.ReplyTo"/>.
    /// </summary>
    public sealed class ReplyingOrderStatusConsumer(IPublisher publisher) : IConsumer<GetOrderStatus>
    {
        public async Task Consume(GetOrderStatus message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            if (context.ReplyTo is null) return;
            await publisher.SendAsync<OrderStatusReply>(context.ReplyTo,
                new OrderStatusReply(message.OrderId, "Shipped"),
                cancellationToken);
        }
    }

    /// <summary>
    /// Captures the <see cref="IConsumeContext.ReplyTo"/> URI and then sends a placeholder reply so the
    /// request client does not wait forever.
    /// </summary>
    public sealed class CapturingReplyToConsumer(
        ReplyToCapture capture,
        IPublisher publisher) : IConsumer<GetOrderStatus>
    {
        public async Task Consume(GetOrderStatus message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            capture.Value = context.ReplyTo;
            if (context.ReplyTo is null) return;
            await publisher.SendAsync<OrderStatusReply>(context.ReplyTo,
                new OrderStatusReply(message.OrderId, "Captured"),
                cancellationToken);
        }
    }

    /// <summary>
    /// Captures a custom request header and sends a reply.
    /// </summary>
    public sealed class HeaderCapturingConsumer(
        HeaderCapture2 capture,
        IPublisher publisher) : IConsumer<GetOrderStatus>
    {
        public async Task Consume(GetOrderStatus message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            capture.Value = context.Headers.GetValueOrDefault("x-custom");
            if (context.ReplyTo is null) return;
            await publisher.SendAsync<OrderStatusReply>(context.ReplyTo,
                new OrderStatusReply(message.OrderId, "Ok"),
                cancellationToken);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartAsync(
        Action<IServiceCollection> registerSingletons,
        Action<InterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        registerSingletons(services);
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RequestAsync_ConsumerReplies_ReturnsReply()
    {
        var (provider, cts) = await StartAsync(
            _ => { },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<ReplyingOrderStatusConsumer>();
                bus.AddRequestClient<ReplyingOrderStatusConsumer, GetOrderStatus, OrderStatusReply>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<GetOrderStatus, OrderStatusReply>>();

            var orderId = Guid.NewGuid();
            var reply = await client.RequestAsync(
                new GetOrderStatus(orderId),
                TestContext.Current.CancellationToken);

            Assert.Equal(orderId, reply.OrderId);
            Assert.Equal("Shipped", reply.Status);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task RequestAsync_ReplyToHeaderIsSet_ConsumerCanReadIt()
    {
        var capturedReplyTo = new ReplyToCapture();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(capturedReplyTo),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<CapturingReplyToConsumer>();
                bus.AddRequestClient<CapturingReplyToConsumer, GetOrderStatus, OrderStatusReply>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<GetOrderStatus, OrderStatusReply>>();

            await client.RequestAsync(
                new GetOrderStatus(Guid.NewGuid()),
                TestContext.Current.CancellationToken);

            Assert.NotNull(capturedReplyTo.Value);
            Assert.Equal(InMemoryBroker.QueueScheme, capturedReplyTo.Value!.Scheme);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task RequestAsync_WithConfigureDelegate_HeadersForwardedToConsumer()
    {
        var capturedHeaders = new HeaderCapture2();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(capturedHeaders),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<HeaderCapturingConsumer>();
                bus.AddRequestClient<HeaderCapturingConsumer, GetOrderStatus, OrderStatusReply>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<GetOrderStatus, OrderStatusReply>>();

            await client.RequestAsync(
                new GetOrderStatus(Guid.NewGuid()),
                ctx => ctx.Headers["x-custom"] = "hello",
                TestContext.Current.CancellationToken);

            Assert.Equal("hello", capturedHeaders.Value);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
