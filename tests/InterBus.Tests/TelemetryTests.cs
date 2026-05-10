using System.Diagnostics;
using InterBus.Consuming;
using InterBus.DeadLettering;
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
        using var listener = BuildListener(onStarted: started.Add);

        var context = BuildDispatchContext();
        var middleware = BuildDispatchMiddleware();

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        Assert.Single(started, a => a.OperationName == "NoOpConsumer process" && a.Kind == ActivityKind.Consumer);
    }

    [Fact]
    public async Task DispatchMiddleware_SetsOkStatus_WhenSuccessful()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var context = BuildDispatchContext();
        var middleware = BuildDispatchMiddleware();

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask,
            TestContext.Current.CancellationToken);

        var act = Assert.Single(stopped);
        Assert.Equal(ActivityStatusCode.Ok, act.Status);
    }

    [Fact]
    public async Task DispatchMiddleware_SetsErrorStatus_WhenConsumerThrows()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var context = BuildDispatchContext();
        var middleware = BuildDispatchMiddleware();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(context, (_, _) => throw new InvalidOperationException("boom"),
                TestContext.Current.CancellationToken));

        var act = Assert.Single(stopped);
        Assert.Equal(ActivityStatusCode.Error, act.Status);
        Assert.Equal("boom", act.StatusDescription);
    }

    [Fact]
    public async Task DispatchMiddleware_DoesNotSetErrorStatus_WhenOperationCancelled()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var context = BuildDispatchContext();
        var middleware = BuildDispatchMiddleware();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            middleware.InvokeAsync(context, (_, _) => throw new OperationCanceledException(),
                TestContext.Current.CancellationToken));

        var act = Assert.Single(stopped);
        Assert.Equal(ActivityStatusCode.Unset, act.Status);
    }

    [Fact]
    public async Task DispatchMiddleware_SetsErrorStatus_WhenDeadLetterException()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var context = BuildDispatchContext();
        var middleware = BuildDispatchMiddleware();

        await Assert.ThrowsAsync<DeadLetterException>(() =>
            middleware.InvokeAsync(context,
                (_, _) => throw new DeadLetterException("dead-reason", "err", DateTimeOffset.UtcNow, new Exception()),
                TestContext.Current.CancellationToken));

        var act = Assert.Single(stopped);
        Assert.Equal(ActivityStatusCode.Error, act.Status);
        Assert.Equal("dead-reason", act.StatusDescription);
    }

    // ── InterBusTelemetry.StartConsumerActivity ──────────────────────────────

    [Fact]
    public void StartConsumerActivity_StartsConsumerActivity_WhenListenerRegistered()
    {
        var started = new List<Activity>();
        using var listener = BuildListener(started.Add);

        var diagnostics = new StubDiagnostics();
        var headers = new MessageHeaders
        {
            [InterBusHeaders.MessageType] = "TestMessage",
        };

        using var activity = InterBusTelemetry.StartConsumerActivity(
            diagnostics, headers, "NoOpConsumer", "test-queue");

        Assert.Single(started, a =>
            a.OperationName == "NoOpConsumer process" && a.Kind == ActivityKind.Consumer);
    }

    [Fact]
    public void StartConsumerActivity_SetsConsumerActivityTags()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var diagnostics = new StubDiagnostics();
        var headers = new MessageHeaders
        {
            [InterBusHeaders.MessageType] = "TestMessage",
            [InterBusHeaders.MessageId] = "msg-123",
        };

        {
            using var activity = InterBusTelemetry.StartConsumerActivity(
                diagnostics, headers, "NoOpConsumer", "test-queue");
        }

        var act = Assert.Single(stopped);
        Assert.Equal("interbus", GetTag(act, "messaging.system"));
        Assert.Equal("process", GetTag(act, "messaging.operation.type"));
        Assert.Equal("test-queue", GetTag(act, "messaging.destination.name"));
        Assert.Equal("NoOpConsumer", GetTag(act, "messaging.consumer.name"));
        Assert.Equal("TestMessage", GetTag(act, "messaging.message.type"));
        Assert.Equal("msg-123", GetTag(act, "messaging.message.id"));
    }

    [Fact]
    public void StartConsumerActivity_RestoresParentContext_FromHeaders()
    {
        var started = new List<Activity>();
        using var listener = BuildListener(started.Add);

        var traceId = ActivityTraceId.CreateRandom();
        var parentSpanId = ActivitySpanId.CreateRandom();
        var traceparent = $"00-{traceId}-{parentSpanId}-01";

        var diagnostics = new StubDiagnostics();
        var headers = new MessageHeaders
        {
            [InterBusHeaders.Traceparent] = traceparent,
        };

        using var activity = InterBusTelemetry.StartConsumerActivity(
            diagnostics, headers, "NoOpConsumer", "test-queue");

        var act = Assert.Single(started, a => a.OperationName == "NoOpConsumer process");
        Assert.Equal(traceId, act.TraceId);
        Assert.Equal(parentSpanId, act.ParentSpanId);
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
        Assert.True(headers.ContainsKey(InterBusHeaders.Traceparent),
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

    private static TelemetryDispatchMiddleware BuildDispatchMiddleware() =>
        new(new StubDiagnostics(), new InterBusMetrics());

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
