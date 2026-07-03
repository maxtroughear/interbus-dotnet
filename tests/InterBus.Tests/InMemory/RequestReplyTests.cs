using System.Diagnostics;
using System.Diagnostics.Metrics;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory;
using InterBus.InMemory.Extensions;
using InterBus.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public record GetOrderStatus(Guid OrderId);

public record OrderStatusReply(Guid OrderId, string Status);

public record OrderStatusUnexpectedReply(Guid OrderId);

public class RequestReplyTests
{
    // ── Supporting types ──────────────────────────────────────────────────────

    public sealed class ReplyToCapture
    {
        public Uri? Value { get; set; }
    }

    public sealed class RequestHeaderCapture
    {
        public string? Value { get; set; }
    }

    /// <summary>
    /// Consumes <see cref="GetOrderStatus"/> and sends a reply using <see cref="IConsumeContext.ReplyTo"/>.
    /// </summary>
    public sealed class ReplyingOrderStatusConsumer(IPublisher publisher) : IConsumer<GetOrderStatus>
    {
        public async Task Consume(GetOrderStatus message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            if (context.ReplyTo is null) return;
            await publisher.SendAsync(context.ReplyTo,
                new OrderStatusReply(message.OrderId, "Shipped"),
                cancellationToken);
        }
    }

    /// <summary>
    /// Captures the <see cref="IConsumeContext.ReplyTo"/> URI and sends a placeholder reply so the
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
            await publisher.SendAsync(context.ReplyTo,
                new OrderStatusReply(message.OrderId, "Captured"),
                cancellationToken);
        }
    }

    /// <summary>
    /// Captures a custom request header and sends a reply.
    /// </summary>
    public sealed class HeaderCapturingConsumer(
        RequestHeaderCapture capture,
        IPublisher publisher) : IConsumer<GetOrderStatus>
    {
        public async Task Consume(GetOrderStatus message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            capture.Value = context.Headers.GetValueOrDefault("x-custom");
            if (context.ReplyTo is null) return;
            await publisher.SendAsync(context.ReplyTo,
                new OrderStatusReply(message.OrderId, "Ok"),
                cancellationToken);
        }
    }

    public sealed class UnexpectedReplyConsumer(
        RequestHeaderCapture capture,
        IPublisher publisher) : IConsumer<GetOrderStatus>
    {
        public async Task Consume(GetOrderStatus message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            capture.Value = context.Headers.GetValueOrDefault("x-custom");
            if (context.ReplyTo is null) return;
            await publisher.SendAsync(context.ReplyTo,
                new OrderStatusUnexpectedReply(message.OrderId),
                cancellationToken);

            await Task.Delay(100, cancellationToken);

            await publisher.SendAsync(context.ReplyTo,
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
            try
            {
                await svc.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
            }
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
        var capturedHeaders = new RequestHeaderCapture();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(capturedHeaders),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<HeaderCapturingConsumer>();
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

    [Fact]
    public async Task RequestAsync_UnexpectedMessages_AreIgnored()
    {
        var capturedHeaders = new RequestHeaderCapture();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(capturedHeaders),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<UnexpectedReplyConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<GetOrderStatus, OrderStatusReply>>();

            var orderId = Guid.NewGuid();
            var reply = await client.RequestAsync(
                new GetOrderStatus(orderId),
                ctx => ctx.Headers["x-custom"] = "hello",
                TestContext.Current.CancellationToken);

            Assert.Equal("hello", capturedHeaders.Value);
            Assert.Equal(orderId, reply.OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task RequestAsync_EmitsRequestAndReplyActivities_WithTelemetryEvents()
    {
        var started = new List<Activity>();
        var stopped = new List<Activity>();

        var (provider, cts) = await StartAsync(
            _ => { },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<ReplyingOrderStatusConsumer>();
            });

        using var listener = BuildTelemetryListener(started.Add, stopped.Add);

        try
        {
            using var scope = provider.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<GetOrderStatus, OrderStatusReply>>();
            await client.RequestAsync(new GetOrderStatus(Guid.NewGuid()), TestContext.Current.CancellationToken);
        }
        finally
        {
            await StopAsync(provider, cts);
        }

        var stoppedActivities = stopped.ToArray();

        var requestActivity = Assert.Single(stoppedActivities, a =>
            a.Kind == ActivityKind.Client &&
            GetTag(a, "messaging.operation.type") == "request");

        var requestEventNames = requestActivity.Events.Select(e => e.Name).ToArray();
        Assert.Contains("interbus.request.started", requestEventNames);
        Assert.Contains("interbus.request.published", requestEventNames);
        Assert.Contains("interbus.reply.received", requestEventNames);
    }

    [Fact]
    public async Task RequestAsync_RecordsRequestAndReplyMetrics()
    {
        var relevantCounters = new HashSet<string>(StringComparer.Ordinal)
        {
            "interbus.requests.started",
            "interbus.requests.completed",
        };
        var relevantHistograms = new HashSet<string>(StringComparer.Ordinal)
        {
            "interbus.request.duration",
        };

        var counters = new Dictionary<string, long>(StringComparer.Ordinal);
        var histogramMeasurements = new Dictionary<string, int>(StringComparer.Ordinal);

        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name != InterBusTelemetry.MeterName)
                return;
            if (relevantCounters.Contains(instrument.Name) || relevantHistograms.Contains(instrument.Name))
                listener.EnableMeasurementEvents(instrument);
        };
        meterListener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
            counters[instrument.Name] = counters.GetValueOrDefault(instrument.Name) + measurement);
        meterListener.SetMeasurementEventCallback<double>((instrument, _, _, _) =>
            histogramMeasurements[instrument.Name] = histogramMeasurements.GetValueOrDefault(instrument.Name) + 1);
        meterListener.Start();

        var (provider, cts) = await StartAsync(
            _ => { },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<ReplyingOrderStatusConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<GetOrderStatus, OrderStatusReply>>();
            await client.RequestAsync(new GetOrderStatus(Guid.NewGuid()), TestContext.Current.CancellationToken);
        }
        finally
        {
            await StopAsync(provider, cts);
        }

        Assert.True(counters.GetValueOrDefault("interbus.requests.started") >= 1);
        Assert.True(counters.GetValueOrDefault("interbus.requests.completed") >= 1);
        Assert.True(histogramMeasurements.GetValueOrDefault("interbus.request.duration") >= 1);
    }

    private static ActivityListener BuildTelemetryListener(
        Action<Activity>? onStarted = null,
        Action<Activity>? onStopped = null)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = src => src.Name == InterBusTelemetry.ActivitySourceName,
            Sample = (ref _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        if (onStarted is not null) listener.ActivityStarted = onStarted;
        if (onStopped is not null) listener.ActivityStopped = onStopped;
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static string? GetTag(Activity activity, string key) =>
        activity.Tags.FirstOrDefault(t => t.Key == key).Value;
}
