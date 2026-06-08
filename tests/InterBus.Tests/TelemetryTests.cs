using System.Diagnostics;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.Sagas;
using InterBus.Telemetry;
using InterBus.Transporting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
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

        using (BuildListener(onStarted: started.Add))
        {
            var context = BuildDispatchContext();
            var middleware = BuildDispatchMiddleware();

            await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask,
                TestContext.Current.CancellationToken);
        }

        Assert.Single(started, a => a is { OperationName: "NoOpConsumer process", Kind: ActivityKind.Consumer });
    }

    [Fact]
    public async Task DispatchMiddleware_SetsOkStatus_WhenSuccessful()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var context = BuildDispatchContext();
            var middleware = BuildDispatchMiddleware();

            await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask,
                TestContext.Current.CancellationToken);
        }

        var act = Assert.Single(stopped, a => a.OperationName == "NoOpConsumer process");
        Assert.Equal(ActivityStatusCode.Ok, act.Status);
    }

    [Fact]
    public async Task DispatchMiddleware_SetsErrorStatus_WhenConsumerThrows()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var context = BuildDispatchContext();
            var middleware = BuildDispatchMiddleware();

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                middleware.InvokeAsync(context, (_, _) => throw new InvalidOperationException("boom"),
                    TestContext.Current.CancellationToken));
        }

        var act = Assert.Single(stopped, a => a.OperationName == "NoOpConsumer process");
        Assert.Equal(ActivityStatusCode.Error, act.Status);
        Assert.Equal("boom", act.StatusDescription);
    }

    [Fact]
    public async Task DispatchMiddleware_DoesNotSetErrorStatus_WhenOperationCancelled()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var context = BuildDispatchContext();
            var middleware = BuildDispatchMiddleware();

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                middleware.InvokeAsync(context, (_, _) => throw new OperationCanceledException(),
                    TestContext.Current.CancellationToken));
        }

        var act = Assert.Single(stopped, a => a.OperationName == "NoOpConsumer process");
        Assert.Equal(ActivityStatusCode.Unset, act.Status);
    }

    [Fact]
    public async Task DispatchMiddleware_SetsErrorStatus_WhenDeadLetterRequested()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var context = BuildDispatchContext();
            var middleware = BuildDispatchMiddleware();

            await middleware.InvokeAsync(context,
                (ctx, _) =>
                {
                    ctx.Result = DispatchResult.DeadLetter(DateTimeOffset.UtcNow,
                        new Exception());
                    return Task.CompletedTask;
                },
                TestContext.Current.CancellationToken);
        }

        var act = Assert.Single(stopped, a => a.OperationName == "NoOpConsumer process");
        Assert.Equal(ActivityStatusCode.Error, act.Status);
        Assert.Equal("Exception of type 'System.Exception' was thrown.", act.StatusDescription);
    }

    // ── InterBusTelemetry.StartConsumerActivity ──────────────────────────────

    [Fact]
    public void StartConsumerActivity_StartsConsumerActivity_WhenListenerRegistered()
    {
        var started = new List<Activity>();

        using (BuildListener(started.Add))
        {
            var diagnostics = new StubDiagnostics();
            var headers = new MessageHeaders
            {
                [InterBusHeaders.MessageType] = "TestMessage",
            };

            using var activity = InterBusTelemetry.StartConsumerActivity(
                diagnostics, headers, "NoOpConsumer", "test-queue");
        }

        Assert.Single(started, a =>
            a is { OperationName: "NoOpConsumer process", Kind: ActivityKind.Consumer });
    }

    [Fact]
    public void StartConsumerActivity_SetsConsumerActivityTags()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
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
        }

        var act = Assert.Single(stopped, a => a.OperationName == "NoOpConsumer process");
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
        var traceId = ActivityTraceId.CreateRandom();
        var parentSpanId = ActivitySpanId.CreateRandom();

        using (BuildListener(started.Add))
        {
            var traceparent = $"00-{traceId}-{parentSpanId}-01";

            var diagnostics = new StubDiagnostics();
            var headers = new MessageHeaders
            {
                [InterBusHeaders.Traceparent] = traceparent,
            };

            using var activity = InterBusTelemetry.StartConsumerActivity(
                diagnostics, headers, "NoOpConsumer", "test-queue");
        }

        var act = Assert.Single(started, a => a.OperationName == "NoOpConsumer process");
        Assert.Equal(traceId, act.TraceId);
        Assert.Equal(parentSpanId, act.ParentSpanId);
    }

    // ── TelemetryPublishMiddleware ────────────────────────────────────────────

    [Fact]
    public async Task PublishMiddleware_StartsProducerActivity_WhenListenerRegistered()
    {
        var started = new List<Activity>();

        using (BuildListener(started.Add))
        {
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
        }

        Assert.Contains(started, a => a.Kind == ActivityKind.Producer);
    }

    [Fact]
    public async Task PublishMiddleware_InjectsTraceparentHeader_IntoOutgoingMessage()
    {
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

        using (BuildListener())
        {
            await publisher.PublishAsync(new TestMessage(), TestContext.Current.CancellationToken);
        }

        Assert.Single(transport.Sends);
        var (_, _, headers) = transport.Sends[0];
        Assert.True(headers.ContainsKey(InterBusHeaders.Traceparent),
            "Expected 'traceparent' header to be injected by TelemetryPublishMiddleware");
    }

    // ── ConsumerExecutor ─────────────────────────────────────────────────────

    [Fact]
    public async Task ConsumerExecutor_StartsChildActivity_WhenListenerRegistered()
    {
        var started = new List<Activity>();

        using (BuildListener(onStarted: started.Add))
        {
            var consumer = new NoOpConsumer();
            var executor = new ConsumerExecutor<NoOpConsumer, TestMessage>(new StubDiagnostics(), consumer);
            var context = BuildConsumeContext();

            await executor.ExecuteAsync(context, TestContext.Current.CancellationToken);
        }

        Assert.Single(started,
            a => a is { OperationName: "NoOpConsumer consume", Kind: ActivityKind.Internal });
    }

    [Fact]
    public async Task ConsumerExecutor_SetsOkStatus_WhenSuccessful()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var executor = new ConsumerExecutor<NoOpConsumer, TestMessage>(new StubDiagnostics(), new NoOpConsumer());
            var context = BuildConsumeContext();

            await executor.ExecuteAsync(context, TestContext.Current.CancellationToken);
        }

        var act = Assert.Single(stopped, a => a.OperationName == "NoOpConsumer consume");
        Assert.Equal(ActivityStatusCode.Ok, act.Status);
    }

    [Fact]
    public async Task ConsumerExecutor_SetsErrorStatus_WhenConsumerThrows()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var executor =
                new ConsumerExecutor<ThrowingConsumer, TestMessage>(new StubDiagnostics(), new ThrowingConsumer());
            var context = BuildConsumeContext();

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                executor.ExecuteAsync(context, TestContext.Current.CancellationToken));
        }

        var act = Assert.Single(stopped, a => a.OperationName == "ThrowingConsumer consume");
        Assert.Equal(ActivityStatusCode.Error, act.Status);
        Assert.Equal("boom", act.StatusDescription);
    }

    [Fact]
    public async Task ConsumerExecutor_DoesNotSetErrorStatus_WhenOperationCancelled()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var consumer = new CancellingConsumer();
            var executor = new ConsumerExecutor<CancellingConsumer, TestMessage>(new StubDiagnostics(), consumer);
            var context = BuildConsumeContext();

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                executor.ExecuteAsync(context, TestContext.Current.CancellationToken));
        }

        var act = Assert.Single(stopped, a => a.OperationName == "CancellingConsumer consume");
        Assert.Equal(ActivityStatusCode.Unset, act.Status);
    }

    // ── SagaExecutor ─────────────────────────────────────────────────────────

    [Fact]
    public async Task SagaExecutor_StartsChildActivity_WhenListenerRegistered()
    {
        var started = new List<Activity>();

        using (BuildListener(onStarted: started.Add))
        {
            var executor = BuildSagaExecutor<NoOpSaga, NoOpSagaState>();

            await executor.ExecuteAsync(BuildConsumeContext(), TestContext.Current.CancellationToken);
        }

        Assert.Single(started,
            a => a is { OperationName: "NoOpSaga consume", Kind: ActivityKind.Internal });
    }

    [Fact]
    public async Task SagaExecutor_SetsOkStatus_WhenSuccessful()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var executor = BuildSagaExecutor<NoOpSaga, NoOpSagaState>();

            await executor.ExecuteAsync(BuildConsumeContext(), TestContext.Current.CancellationToken);
        }

        var act = Assert.Single(stopped, a => a.OperationName == "NoOpSaga consume");
        Assert.Equal(ActivityStatusCode.Ok, act.Status);
    }

    [Fact]
    public async Task SagaExecutor_SetsErrorStatus_WhenConsumeThrows()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var executor = BuildSagaExecutor<ThrowingSaga, NoOpSagaState>();

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                executor.ExecuteAsync(BuildConsumeContext(), TestContext.Current.CancellationToken));
        }

        var act = Assert.Single(stopped, a => a.OperationName == "ThrowingSaga consume");
        Assert.Equal(ActivityStatusCode.Error, act.Status);
        Assert.Equal("saga boom", act.StatusDescription);
    }

    [Fact]
    public async Task SagaExecutor_DoesNotSetErrorStatus_WhenOperationCancelled()
    {
        var stopped = new List<Activity>();

        using (BuildListener(onStopped: stopped.Add))
        {
            var executor = BuildSagaExecutor<CancellingSaga, NoOpSagaState>();

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                executor.ExecuteAsync(BuildConsumeContext(), TestContext.Current.CancellationToken));
        }

        var act = Assert.Single(stopped, a => a.OperationName == "CancellingSaga consume");
        Assert.Equal(ActivityStatusCode.Unset, act.Status);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ActivityListener BuildListener(
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

    private static TelemetryDispatchMiddleware BuildDispatchMiddleware() =>
        new(new StubDiagnostics(), new InterBusMetrics());

    private static DispatchContext BuildDispatchContext(MessageHeaders? headers = null)
    {
        var descriptor = new ConsumerDescriptor(
            typeof(NoOpConsumer),
            RoutingConventions.QueueUri("queue", "test-queue"));

        return new DispatchContext(descriptor, [], headers ?? new MessageHeaders());
    }

    private static string? GetTag(Activity activity, string key) =>
        activity.Tags.FirstOrDefault(t => t.Key == key).Value;

    private static ConsumeContext<TestMessage> BuildConsumeContext()
    {
        var descriptor = new ConsumerDescriptor(
            typeof(NoOpConsumer),
            RoutingConventions.QueueUri("queue", "test-queue"));
        return new ConsumeContext<TestMessage>(descriptor, new TestMessage(), new MessageHeaders());
    }

    private static SagaExecutor<TSaga, TState, TestMessage> BuildSagaExecutor<TSaga, TState>()
        where TSaga : class, ISaga<TState>, ISagaConsumer<TState, TestMessage>, new()
        where TState : class, new()
        => new(
            new TSaga(),
            new StubSagaRepository<TSaga, TState>(),
            new SagaOptions<TSaga>(),
            new StubDiagnostics(),
            NullLogger<SagaExecutor<TSaga, TState, TestMessage>>.Instance);

    private sealed record TestMessage;

    private sealed class NoOpConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private sealed class ThrowingConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("boom");
    }

    private sealed class CancellingConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default)
            => throw new OperationCanceledException();
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

    private sealed class NoOpSagaState;

    private sealed class NoOpSaga :
        ISagaConsumer<NoOpSagaState, TestMessage>
    {
        public string GetCorrelationId(TestMessage message) => "test";

        public Task Consume(TestMessage message, ISagaContext<NoOpSagaState> context,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    private sealed class ThrowingSaga :
        ISagaConsumer<NoOpSagaState, TestMessage>
    {
        public string GetCorrelationId(TestMessage message) => "test";

        public Task Consume(TestMessage message, ISagaContext<NoOpSagaState> context,
            CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("saga boom");
    }

    private sealed class CancellingSaga :
        ISagaConsumer<NoOpSagaState, TestMessage>
    {
        public string GetCorrelationId(TestMessage message) => "test";

        public Task Consume(TestMessage message, ISagaContext<NoOpSagaState> context,
            CancellationToken cancellationToken = default)
            => throw new OperationCanceledException();
    }

    private sealed class StubSagaRepository<TSaga, TState> : ISagaRepository<TSaga, TState>
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        public Task<IAsyncDisposable> AcquireLockAsync(string correlationId,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IAsyncDisposable>(new NullAsyncDisposable());

        public Task<SagaEntry<TState>?> GetAsync(string correlationId,
            CancellationToken cancellationToken = default)
            => Task.FromResult<SagaEntry<TState>?>(null);

        public Task SaveAsync(string correlationId, SagaEntry<TState> entry,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task DeleteAsync(string correlationId,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        private sealed class NullAsyncDisposable : IAsyncDisposable
        {
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
