using System.Diagnostics;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.Telemetry;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for InterBus telemetry — <see cref="IInterBusDiagnostics"/>, <see cref="InterBusTelemetry"/>,
/// <see cref="TelemetryDispatchMiddleware"/>, and <see cref="TelemetryPublishMiddleware"/>.
/// </summary>
public sealed class TelemetryTests
{
    // ── InterBusTelemetry constants ──────────────────────────────────────────

    [Fact]
    public void ActivitySourceName_IsInterBus()
    {
        Assert.Equal("InterBus", InterBusTelemetry.ActivitySourceName);
    }

    [Fact]
    public void MeterName_IsInterBus()
    {
        Assert.Equal("InterBus", InterBusTelemetry.MeterName);
    }

    // ── IInterBusDiagnostics / InterBusDiagnostics ───────────────────────────

    [Fact]
    public void AddInterBus_RegistersIInterBusDiagnostics()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<IInterBusDiagnostics>());
    }

    [Fact]
    public void IInterBusDiagnostics_ExposesActivitySourceWithCorrectName()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });

        using var provider = services.BuildServiceProvider();
        var diagnostics = provider.GetRequiredService<IInterBusDiagnostics>();

        Assert.Equal(InterBusTelemetry.ActivitySourceName, diagnostics.ActivitySource.Name);
    }

    [Fact]
    public void IInterBusDiagnostics_IsSingleton()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });

        using var provider = services.BuildServiceProvider();
        var a = provider.GetRequiredService<IInterBusDiagnostics>();
        var b = provider.GetRequiredService<IInterBusDiagnostics>();

        Assert.Same(a, b);
    }

    // ── TelemetryDispatchMiddleware ───────────────────────────────────────────

    [Fact]
    public async Task DispatchMiddleware_StartsConsumerActivity_WhenListenerRegistered()
    {
        var started = new List<Activity>();
        using var listener = BuildListener(started.Add);

        var middleware = BuildDispatchMiddleware();
        var context = BuildDispatchContext(new MessageHeaders
        {
            [InterBusHeaders.MessageType] = "TestMessage",
        });

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        Assert.Single(started, a =>
            a.OperationName == "NoOpConsumer process" && a.Kind == ActivityKind.Consumer);
    }

    [Fact]
    public async Task DispatchMiddleware_SetsConsumerActivityTags()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var middleware = BuildDispatchMiddleware();
        var context = BuildDispatchContext(new MessageHeaders
        {
            [InterBusHeaders.MessageType] = "TestMessage",
            [InterBusHeaders.MessageId] = "msg-123",
        });

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        var activity = Assert.Single(stopped);
        Assert.Equal("interbus", GetTag(activity, "messaging.system"));
        Assert.Equal("process", GetTag(activity, "messaging.operation.type"));
        Assert.Equal("test-queue", GetTag(activity, "messaging.destination.name"));
        Assert.Equal("NoOpConsumer", GetTag(activity, "messaging.consumer.name"));
        Assert.Equal("TestMessage", GetTag(activity, "messaging.message.type"));
        Assert.Equal("msg-123", GetTag(activity, "messaging.message.id"));
    }

    [Fact]
    public async Task DispatchMiddleware_SetsErrorStatus_WhenConsumerThrows()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var middleware = BuildDispatchMiddleware();
        var context = BuildDispatchContext();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(context, (_, _) => throw new InvalidOperationException("boom"),
                TestContext.Current.CancellationToken));

        var activity = Assert.Single(stopped);
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("boom", activity.StatusDescription);
    }

    [Fact]
    public async Task DispatchMiddleware_RestoresParentContext_FromTraceparentHeader()
    {
        var started = new List<Activity>();
        using var listener = BuildListener(started.Add);

        var traceId = ActivityTraceId.CreateRandom();
        var parentSpanId = ActivitySpanId.CreateRandom();
        var traceparent = $"00-{traceId}-{parentSpanId}-01";

        var middleware = BuildDispatchMiddleware();
        var context = BuildDispatchContext(new MessageHeaders
        {
            ["traceparent"] = traceparent,
        });

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        var activity = Assert.Single(started);
        Assert.Equal(traceId, activity.TraceId);
        Assert.Equal(parentSpanId, activity.ParentSpanId);
    }

    // ── TelemetryPublishMiddleware ────────────────────────────────────────────

    [Fact]
    public async Task PublishMiddleware_StartsProducerActivity_WhenListenerRegistered()
    {
        var started = new List<Activity>();
        using var listener = BuildListener(started.Add);

        var transport = new CapturingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddKeyedSingleton<ITransport>("topic", transport);
            bus.Services.AddKeyedSingleton<ITransport>("queue", transport);
        });

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        await publisher.PublishAsync(new TestMessage(), TestContext.Current.CancellationToken);

        Assert.Contains(started, a => a.Kind == ActivityKind.Producer);
    }

    [Fact]
    public async Task PublishMiddleware_InjectsTraceparentHeader_IntoOutgoingMessage()
    {
        using var listener = BuildListener();

        var transport = new CapturingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddKeyedSingleton<ITransport>("topic", transport);
            bus.Services.AddKeyedSingleton<ITransport>("queue", transport);
        });

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        await publisher.PublishAsync(new TestMessage(), TestContext.Current.CancellationToken);

        Assert.Single(transport.Sends);
        var (_, _, headers) = transport.Sends[0];
        Assert.True(headers.ContainsKey("traceparent"),
            "Expected 'traceparent' header to be injected by TelemetryPublishMiddleware");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ActivityListener BuildListener(
        Action<Activity>? onStarted = null,
        Action<Activity>? onStopped = null)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = src => src.Name == InterBusTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        if (onStarted is not null) listener.ActivityStarted = onStarted;
        if (onStopped is not null) listener.ActivityStopped = onStopped;
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static TelemetryDispatchMiddleware BuildDispatchMiddleware()
    {
        var diagnostics = new StubDiagnostics();
        var metrics = new InterBusMetrics();
        return new TelemetryDispatchMiddleware(diagnostics, metrics);
    }

    private static DispatchContext BuildDispatchContext(MessageHeaders? headers = null)
    {
        var descriptor = new ConsumerDescriptor(
            typeof(NoOpConsumer),
            RoutingConventions.QueueUri("queue", "test-queue"),
            [typeof(TestMessage)],
            (_, _, _) => Task.CompletedTask);

        return new DispatchContext(descriptor, [], headers ?? new MessageHeaders());
    }

    private static string? GetTag(Activity activity, string key) =>
        activity.Tags.FirstOrDefault(t => t.Key == key).Value;

    private sealed record TestMessage;

    private sealed class NoOpConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private sealed class CapturingTransport : ITransport
    {
        public List<(Uri Destination, byte[] Payload, MessageHeaders Headers)> Sends { get; } = [];

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            Sends.Add((context.Destination, context.Payload, context.Headers));
            return Task.CompletedTask;
        }
    }

    private sealed class StubDiagnostics : IInterBusDiagnostics
    {
        public ActivitySource ActivitySource { get; } = new(InterBusTelemetry.ActivitySourceName);
    }
}
