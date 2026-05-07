using System.Diagnostics;
using InterBus.Consuming;
using InterBus.Extensions;
using InterBus.Telemetry;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for InterBus telemetry — <see cref="IInterBusDiagnostics"/>, <see cref="InterBusTelemetry"/>,
/// and activity/metrics integration in <see cref="ConsumePipelineExecutor"/> and
/// <c>PublishPipelineExecutor</c>.
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
        var diagnostics = provider.GetService<IInterBusDiagnostics>();

        Assert.NotNull(diagnostics);
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

    // ── ConsumePipelineExecutor starts a Consumer activity ───────────────────

    [Fact]
    public async Task ConsumePipelineExecutor_StartsConsumerActivity_WhenListenerRegistered()
    {
        var activities = new List<Activity>();
        using var listener = BuildListener(activities.Add);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
        var context = BuildConsumeContext(headers: new MessageHeaders
        {
            [InterBusHeaders.MessageType] = "TestMessage",
        });

        await executor.ExecuteAsync(context, (_, _, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Single(activities, a =>
            a.OperationName == "NoOpConsumer process" && a.Kind == ActivityKind.Consumer);
    }

    [Fact]
    public async Task ConsumePipelineExecutor_SetsConsumerActivityTags()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
        var context = BuildConsumeContext(headers: new MessageHeaders
        {
            [InterBusHeaders.MessageType] = "TestMessage",
            [InterBusHeaders.MessageId] = "msg-123",
        });

        await executor.ExecuteAsync(context, (_, _, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        var activity = Assert.Single(stopped);
        Assert.Equal("interbus", GetTag(activity, "messaging.system"));
        Assert.Equal("process", GetTag(activity, "messaging.operation.type"));
        Assert.Equal("test-queue", GetTag(activity, "messaging.destination.name"));
        Assert.Equal("NoOpConsumer", GetTag(activity, "messaging.consumer.name"));
        Assert.Equal("TestMessage", GetTag(activity, "messaging.message.type"));
        Assert.Equal("msg-123", GetTag(activity, "messaging.message.id"));
    }

    [Fact]
    public async Task ConsumePipelineExecutor_SetsErrorStatus_WhenConsumerThrows()
    {
        var stopped = new List<Activity>();
        using var listener = BuildListener(onStopped: stopped.Add);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
        var context = BuildConsumeContext();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            executor.ExecuteAsync(context, (_, _, _) => throw new InvalidOperationException("boom"),
                TestContext.Current.CancellationToken));

        var activity = Assert.Single(stopped);
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("boom", activity.StatusDescription);
    }

    [Fact]
    public async Task ConsumePipelineExecutor_RestoresParentContext_FromTraceparentHeader()
    {
        var started = new List<Activity>();
        using var listener = BuildListener(started.Add);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(_ => { });

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        var traceId = ActivityTraceId.CreateRandom();
        var parentSpanId = ActivitySpanId.CreateRandom();
        var traceparent = $"00-{traceId}-{parentSpanId}-01";

        var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
        var context = BuildConsumeContext(headers: new MessageHeaders
        {
            ["traceparent"] = traceparent,
        });

        await executor.ExecuteAsync(context, (_, _, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        var activity = Assert.Single(started);
        Assert.Equal(traceId, activity.TraceId);
        Assert.Equal(parentSpanId, activity.ParentSpanId);
    }

    // ── PublishPipelineExecutor starts a Producer activity ───────────────────

    [Fact]
    public async Task PublishPipelineExecutor_StartsProducerActivity_WhenListenerRegistered()
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
    public async Task PublishPipelineExecutor_InjectsTraceparentHeader_IntoOutgoingMessage()
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
            "Expected 'traceparent' header to be injected by PublishPipelineExecutor");
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

    private static ConsumeContext<TestMessage> BuildConsumeContext(MessageHeaders? headers = null)
    {
        var descriptor = new ConsumerDescriptor(
            typeof(NoOpConsumer),
            RoutingConventions.QueueUri("queue", "test-queue"),
            [typeof(TestMessage)],
            (_, _, _) => Task.CompletedTask);

        return new ConsumeContext<TestMessage>(descriptor, new TestMessage(), headers ?? new MessageHeaders());
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
}
