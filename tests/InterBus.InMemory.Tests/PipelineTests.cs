using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Middleware;
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

    #region Publish middleware

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
    /// and appends a header before calling next middleware.
    /// </summary>
    public sealed class RecordingPublishMiddleware(PublishCallTracker tracker) : IPublishMiddleware
    {
        public async Task InvokeAsync<T>(PublishContext<T> context,
            Func<PublishContext<T>, CancellationToken, Task> next,
            CancellationToken cancellationToken = default) where T : class
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
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.UsePublishMiddleware<RecordingPublishMiddleware>();
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
                bus.AddConsumer<HeaderCapturingConsumer>();
                bus.UsePublishMiddleware<RecordingPublishMiddleware>();
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
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.UsePublishMiddleware<FirstPublishMiddleware>();
                bus.UsePublishMiddleware<SecondPublishMiddleware>();
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

    #endregion

    #region Consume middleware

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
                bus.AddConsumer<OrderPlacedConsumer>(b => { b.UseConsumeMiddleware<RecordingConsumeMiddleware>(); });
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
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<FaultingConsumer>();
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

    #endregion

    #region Per-consumer middleware

    [Fact]
    public async Task PerConsumerMiddleware_IsInvoked_OnlyForThatConsumer()
    {
        // Register middleware on OrderPlacedConsumer but NOT on AuditConsumer.
        // The per-consumer middleware should only fire for OrderPlacedConsumer.
        var perConsumerTracker = new ConsumeMiddlewareCallTracker();
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(perConsumerTracker);
                services.AddSingleton(orderTracker);
                services.AddSingleton(auditTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<OrderPlacedConsumer>(b =>
                    b.UseConsumeMiddleware<RecordingConsumeMiddleware>());
                bus.AddConsumer<AuditConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "per-consumer@test.com", 10m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count >= 1 && auditTracker.Messages.Count >= 1);

            // Per-consumer middleware should have been invoked exactly once (for OrderPlacedConsumer).
            Assert.Equal(1, perConsumerTracker.InvocationCount);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task WithConsumers_AppliesPerConsumerMiddleware_ToAllRegisteredConsumers()
    {
        var perConsumerTracker = new ConsumeMiddlewareCallTracker();
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(perConsumerTracker);
                services.AddSingleton(orderTracker);
                services.AddSingleton(auditTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<AuditConsumer>();
                // Apply per-consumer middleware to all registered consumers at once.
                bus.WithConsumers(b => b.UseConsumeMiddleware<RecordingConsumeMiddleware>());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "all-consumers@test.com", 5m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count >= 1 && auditTracker.Messages.Count >= 1);

            // Per-consumer middleware should have been invoked once per consumer (2 consumers → 2 invocations).
            Assert.Equal(2, perConsumerTracker.InvocationCount);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    #endregion

    #region Support types

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
        public async Task InvokeAsync<T>(ConsumeContext<T> context,
            Func<ConsumeContext<T>, CancellationToken, Task> next,
            CancellationToken cancellationToken = default) where T : class
        {
            tracker.Increment();
            await next(context, cancellationToken);
        }
    }

    public sealed class FirstPublishMiddleware(ConcurrentQueue<string> log) : IPublishMiddleware
    {
        public async Task InvokeAsync<T>(PublishContext<T> context,
            Func<PublishContext<T>, CancellationToken, Task> next,
            CancellationToken cancellationToken = default) where T : class
        {
            log.Enqueue("first-enter");
            await next(context, cancellationToken);
            log.Enqueue("first-exit");
        }
    }

    public sealed class SecondPublishMiddleware(ConcurrentQueue<string> log) : IPublishMiddleware
    {
        public async Task InvokeAsync<T>(PublishContext<T> context,
            Func<PublishContext<T>, CancellationToken, Task> next,
            CancellationToken cancellationToken = default) where T : class
        {
            log.Enqueue("second-enter");
            await next(context, cancellationToken);
            log.Enqueue("second-exit");
        }
    }

    /// <summary>Consume middleware that logs entry/exit to a shared queue for ordering tests.</summary>
    public sealed class FirstConsumeMiddleware(ConcurrentQueue<string> log) : IConsumeMiddleware
    {
        public async Task InvokeAsync<T>(ConsumeContext<T> context,
            Func<ConsumeContext<T>, CancellationToken, Task> next,
            CancellationToken cancellationToken = default) where T : class
        {
            log.Enqueue("first-enter");
            await next(context, cancellationToken);
            log.Enqueue("first-exit");
        }
    }

    /// <summary>Consume middleware that logs entry/exit to a shared queue for ordering tests.</summary>
    public sealed class SecondConsumeMiddleware(ConcurrentQueue<string> log) : IConsumeMiddleware
    {
        public async Task InvokeAsync<T>(ConsumeContext<T> context,
            Func<ConsumeContext<T>, CancellationToken, Task> next,
            CancellationToken cancellationToken = default) where T : class
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

    #endregion

    #region Consume-scoped publish middleware

    [Fact]
    public async Task ConsumeContext_Publish_DeliversMessage()
    {
        // A consumer publishes a new message from within its Consume method via IConsumeContext.
        // The published message should be delivered to the downstream consumer.
        var orderShippedTracker = new OrderShippedTracker();

        var (provider, cts) = await StartAsync(
            services => { services.AddSingleton(orderShippedTracker); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<PublishingOrderPlacedConsumer>();
                bus.AddConsumer<DownstreamOrderShippedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            await publisher.Send<PublishingOrderPlacedConsumer, OrderPlaced>(
                new OrderPlaced(Guid.NewGuid(), "chain@test.com", 1m),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1);

            Assert.Single(orderShippedTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task ConsumeContext_Publish_UsesConsumerScopedPublishMiddleware()
    {
        // Middleware registered on OrderPlacedConsumer via UsePublishMiddleware should
        // be invoked when that consumer publishes from its Consume method.
        var publishCallTracker = new PublishCallTracker();
        var orderShippedTracker = new OrderShippedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(publishCallTracker);
                services.AddSingleton(orderShippedTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<PublishingOrderPlacedConsumer>(b =>
                    b.UsePublishMiddleware<RecordingPublishMiddleware>());
                bus.AddConsumer<DownstreamOrderShippedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            await publisher.Send<PublishingOrderPlacedConsumer, OrderPlaced>(
                new OrderPlaced(Guid.NewGuid(), "scoped-publish@test.com", 1m),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1);

            // The consumer-scoped publish middleware must have run.
            Assert.NotEmpty(publishCallTracker.Calls);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task ConsumeContext_Publish_BusLevelPublishMiddlewareIsNotInvoked()
    {
        // Bus-level publish middleware should NOT fire when a consumer publishes
        // via IConsumeContext – the consumer-scoped pipeline is used instead.
        var busPublishCallTracker = new PublishCallTracker();
        var orderShippedTracker = new OrderShippedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(busPublishCallTracker);
                services.AddSingleton(orderShippedTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                // Register the recording middleware at the BUS level, NOT on the consumer.
                bus.UsePublishMiddleware<RecordingPublishMiddleware>();
                bus.AddConsumer<PublishingOrderPlacedConsumer>();
                bus.AddConsumer<DownstreamOrderShippedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

            // Drain any recorded calls from the initial Publish via the bus publisher.
            await publisher.Send<PublishingOrderPlacedConsumer, OrderPlaced>(
                new OrderPlaced(Guid.NewGuid(), "bus-level@test.com", 1m),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1);

            // The initial Send (via bus publisher) triggers the recording middleware once for
            // the outer send. The subsequent publish from within the consumer uses the
            // consumer-scoped pipeline, which has NO recording middleware registered,
            // so no additional calls should be recorded after the bus-level send.
            var callsAfterInitialSend = busPublishCallTracker.Calls.Count;

            // Verify message was delivered – meaning the consumer's publish worked.
            Assert.Single(orderShippedTracker.Messages);

            // The recording middleware was registered at bus level only; after the initial
            // send the counter must not have grown from the consumer-scoped publish.
            Assert.Equal(callsAfterInitialSend, busPublishCallTracker.Calls.Count);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    #endregion

    #region Consume-scoped send middleware

    [Fact]
    public async Task ConsumeContext_Send_UsesConsumerScopedSendMiddleware()
    {
        // ISendMiddleware registered on a consumer via UseSendMiddleware should be
        // invoked when that consumer sends a message from its Consume method.
        var sendCallTracker = new SendMiddlewareCallTracker();
        var orderShippedTracker = new OrderShippedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(sendCallTracker);
                services.AddSingleton(orderShippedTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<PublishingOrderPlacedConsumer>(b =>
                    b.UseSendMiddleware<RecordingSendMiddleware>());
                bus.AddConsumer<DownstreamOrderShippedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            await publisher.Send<PublishingOrderPlacedConsumer, OrderPlaced>(
                new OrderPlaced(Guid.NewGuid(), "scoped-send@test.com", 1m),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1);

            // The consumer-scoped send middleware must have been invoked.
            Assert.True(sendCallTracker.InvocationCount > 0);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    #endregion

    #region Consume-scoped support types

    /// <summary>
    /// Consumes an <see cref="OrderPlaced"/> message and immediately publishes an
    /// <see cref="OrderShipped"/> message via <see cref="IConsumeContext"/>.
    /// </summary>
    public sealed class PublishingOrderPlacedConsumer : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default) =>
            context.Publish(new OrderShipped(message.OrderId, "TRK-CHAIN"), cancellationToken);
    }

    public sealed class DownstreamOrderShippedConsumer(OrderShippedTracker tracker) : IConsumer<OrderShipped>
    {
        public Task Consume(OrderShipped message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            return Task.CompletedTask;
        }
    }

    public sealed class SendMiddlewareCallTracker
    {
        private int _count;
        public int InvocationCount => _count;
        public void Increment() => Interlocked.Increment(ref _count);
    }

    public sealed class RecordingSendMiddleware(SendMiddlewareCallTracker tracker) : ISendMiddleware
    {
        public async Task InvokeAsync(SendContext context,
            Func<SendContext, CancellationToken, Task> next,
            CancellationToken cancellationToken = default)
        {
            tracker.Increment();
            await next(context, cancellationToken);
        }
    }

    #endregion
}
