using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

/// <summary>
/// Tests for the publish and consume middleware pipelines.
/// </summary>
public class PipelineTests
{
    /// <summary>
    /// Polls <paramref name="condition"/> up to <paramref name="timeoutMs"/> milliseconds,
    /// checking every <paramref name="intervalMs"/> milliseconds.
    /// </summary>
    private static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 5000,
        int intervalMs = 25)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartAsync(
        Action<IServiceCollection> registerSingletons,
        Action<IInterBusBuilder> configureBus)
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
        cts.Cancel();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    // ─── Publish middleware ───────────────────────────────────────────────────

    /// <summary>
    /// Tracks which destinations were seen by publish middleware so tests can
    /// assert the middleware ran in the expected order.
    /// </summary>
    public sealed class PublishCallTracker
    {
        private readonly ConcurrentBag<string> _calls = [];
        public IReadOnlyCollection<string> Calls => _calls;
        public void Record(string name) => _calls.Add(name);
    }

    /// <summary>
    /// Publish middleware that records a call to <see cref="PublishCallTracker"/>
    /// and appends a header before calling <paramref name="next"/>.
    /// </summary>
    public sealed class RecordingPublishMiddleware(PublishCallTracker tracker) : IPublishMiddleware
    {
        public async Task InvokeAsync(PublishContext context, PublishDelegate next,
            CancellationToken cancellationToken = default)
        {
            tracker.Record("before-" + context.Destination);
            // Demonstrate header mutation (claim-check-like interception point).
            context.Headers["x-middleware-ran"] = "true";
            await next(context, cancellationToken);
            tracker.Record("after-" + context.Destination);
        }
    }

    [Fact]
    public async Task PublishMiddleware_IsInvoked_BeforeTransport()
    {
        var tracker = new PublishCallTracker();
        var orderTracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(tracker);
                services.AddSingleton(orderTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
                bus.AddPublishMiddleware<RecordingPublishMiddleware>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "pipeline@test.com", 10m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1);

            Assert.Single(orderTracker.Messages);
            // Middleware recorded both before and after, confirming it wrapped the send.
            Assert.Contains(tracker.Calls, c => c.StartsWith("before-"));
            Assert.Contains(tracker.Calls, c => c.StartsWith("after-"));
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task PublishMiddleware_HeaderMutation_IsSeenByConsumer()
    {
        // The RecordingPublishMiddleware adds x-middleware-ran: true to every publish.
        // We verify the consumer receives the header via its IConsumeContext.
        var tracker = new PublishCallTracker();
        var headerCapture = new HeaderCapturingTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(tracker);
                services.AddSingleton(headerCapture);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
                bus.AddPublishMiddleware<RecordingPublishMiddleware>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "header@test.com", 5m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => headerCapture.CapturedHeaders.Count == 1);

            Assert.True(headerCapture.CapturedHeaders.Single().TryGetValue("x-middleware-ran", out var value));
            Assert.Equal("true", value);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task MultiplePublishMiddlewares_ExecuteInRegistrationOrder()
    {
        // Register two middlewares; first registered should execute outermost (before second).
        var log = new ConcurrentQueue<string>();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(log);
                services.AddSingleton(new OrderPlacedTracker());
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
                bus.AddPublishMiddleware<FirstPublishMiddleware>();
                bus.AddPublishMiddleware<SecondPublishMiddleware>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "order@test.com", 1m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            // Allow async completion.
            await Task.Delay(50, TestContext.Current.CancellationToken);

            var entries = log.ToList();
            Assert.Equal(4, entries.Count);
            Assert.Equal("first-enter", entries[0]);
            Assert.Equal("second-enter", entries[1]);
            Assert.Equal("second-exit", entries[2]);
            Assert.Equal("first-exit", entries[3]);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    // ─── Consume middleware ───────────────────────────────────────────────────

    [Fact]
    public async Task ConsumeMiddleware_IsInvoked_ForEachMessage()
    {
        var consumeTracker = new ConsumeMiddlewareCallTracker();
        var orderTracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(consumeTracker);
                services.AddSingleton(orderTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
                bus.AddConsumeMiddleware<RecordingConsumeMiddleware>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "consume@test.com", 20m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count >= 1);

            Assert.True(consumeTracker.InvocationCount > 0);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task ConsumeMiddleware_ExceptionHandling_IsLoggedAndDoesNotPropagateToMessagePump()
    {
        // Publish to a consumer that always throws. The default exception-logging
        // middleware should absorb the exception; subsequent messages must still be processed.
        var orderTracker = new OrderPlacedTracker();
        var faultingCount = new FaultingConsumerCallCount();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(orderTracker);
                services.AddSingleton(faultingCount);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

            // Send directly to FaultingConsumer.
            await publisher.Send<FaultingConsumer, OrderPlaced>(
                new OrderPlaced(Guid.NewGuid(), "fault@test.com", 0m),
                TestContext.Current.CancellationToken);

            // Send a normal message to OrderPlacedConsumer to prove the pump is alive.
            var normalMsg = new OrderPlaced(Guid.NewGuid(), "ok@test.com", 5m);
            await publisher.Publish(normalMsg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count >= 1 && faultingCount.Count >= 1);

            // The faulting consumer was called.
            Assert.True(faultingCount.Count >= 1);
            // The message pump recovered and delivered the normal message.
            Assert.Single(orderTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    // ─── Support types ────────────────────────────────────────────────────────

    public sealed class HeaderCapturingTracker
    {
        private readonly ConcurrentBag<IReadOnlyDictionary<string, string>> _captured = [];
        public IReadOnlyCollection<IReadOnlyDictionary<string, string>> CapturedHeaders => _captured;
        public void Capture(IReadOnlyDictionary<string, string> headers) => _captured.Add(headers);
    }

    public sealed class HeaderCapturingConsumer(HeaderCapturingTracker tracker) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Capture(context.Headers);
            return Task.CompletedTask;
        }
    }

    public sealed class ConsumeMiddlewareCallTracker
    {
        private int _count;
        public int InvocationCount => _count;
        public void Increment() => Interlocked.Increment(ref _count);
    }

    public sealed class RecordingConsumeMiddleware(ConsumeMiddlewareCallTracker tracker) : IConsumeMiddleware
    {
        public async Task InvokeAsync(ConsumePipelineContext context, ConsumeDelegate next,
            CancellationToken cancellationToken = default)
        {
            tracker.Increment();
            await next(context, cancellationToken);
        }
    }

    public sealed class FirstPublishMiddleware(ConcurrentQueue<string> log) : IPublishMiddleware
    {
        public async Task InvokeAsync(PublishContext context, PublishDelegate next,
            CancellationToken cancellationToken = default)
        {
            log.Enqueue("first-enter");
            await next(context, cancellationToken);
            log.Enqueue("first-exit");
        }
    }

    public sealed class SecondPublishMiddleware(ConcurrentQueue<string> log) : IPublishMiddleware
    {
        public async Task InvokeAsync(PublishContext context, PublishDelegate next,
            CancellationToken cancellationToken = default)
        {
            log.Enqueue("second-enter");
            await next(context, cancellationToken);
            log.Enqueue("second-exit");
        }
    }

    public sealed class FaultingConsumerCallCount
    {
        private int _count;
        public int Count => _count;
        public void Increment() => Interlocked.Increment(ref _count);
    }

    public sealed class FaultingConsumer(FaultingConsumerCallCount count) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            count.Increment();
            throw new InvalidOperationException("Intentional test exception from FaultingConsumer");
        }
    }
}
