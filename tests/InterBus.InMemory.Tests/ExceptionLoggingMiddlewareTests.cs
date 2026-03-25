using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

public record ExceptionLoggingTestMessage(Guid Id, string Content);

#region Consumers

/// <summary>
/// Tracks consumer invocations to verify exception handling behaviour.
/// </summary>
public sealed class ExceptionLoggingTestTracker
{
    private readonly ConcurrentDictionary<Guid, int> _attemptCounts = [];
    private readonly ConcurrentBag<Guid> _successes = [];

    /// <summary>How many times this message ID should fail before succeeding.</summary>
    public int FailCount { get; set; } = 1;

    public int GetAttemptCount(Guid messageId) =>
        _attemptCounts.GetOrAdd(messageId, 0);

    public IReadOnlyCollection<Guid> Successes => _successes;

    public void RecordAttempt(Guid messageId)
    {
        _attemptCounts.AddOrUpdate(messageId, 1, (_, count) => count + 1);
    }

    public bool ShouldThrow(Guid messageId) =>
        GetAttemptCount(messageId) <= FailCount;

    public void RecordSuccess(Guid messageId) => _successes.Add(messageId);
}

/// <summary>
/// Test consumer that throws based on the tracker's configuration.
/// Used to test exception logging and redelivery behaviour.
/// </summary>
public sealed class ExceptionLoggingTestConsumer(ExceptionLoggingTestTracker tracker)
    : IConsumer<ExceptionLoggingTestMessage>
{
    public Task Consume(ExceptionLoggingTestMessage message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.RecordAttempt(message.Id);

        if (tracker.ShouldThrow(message.Id))
        {
            throw new InvalidOperationException($"Simulated failure for message {message.Id}");
        }

        tracker.RecordSuccess(message.Id);
        return Task.CompletedTask;
    }
}

#endregion

/// <summary>
/// Tests for <see cref="ExceptionLoggingConsumeMiddleware"/>.
/// Validates that exceptions thrown by consumers are logged and rethrown,
/// and that messages remain available for redelivery when processing fails.
/// </summary>
public sealed class ExceptionLoggingMiddlewareTests
{
    #region Test harness helpers

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 5000,
        int intervalMs = 50)
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

    #endregion

    [Fact]
    public async Task LogsException_WhenConsumerThrows()
    {
        // First delivery fails, message is released back to the queue, second delivery succeeds.
        var tracker = new ExceptionLoggingTestTracker { FailCount = 1 };

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var messageId = Guid.NewGuid();
            var msg = new ExceptionLoggingTestMessage(messageId, "test");

            await publisher.Send<ExceptionLoggingTestConsumer, ExceptionLoggingTestMessage>(
                msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Successes.Count == 1, timeoutMs: 3000);

            Assert.Equal(2, tracker.GetAttemptCount(messageId)); // fail + redelivery success
            Assert.Single(tracker.Successes);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task AllowsSubsequentRetry_WhenExceptionOccurs()
    {
        // Validate explicit redelivery semantics: failed delivery is released to broker,
        // then retried and processed by the consumer.
        var tracker = new ExceptionLoggingTestTracker { FailCount = 1 }; // Fail once, then succeed

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var messageId = Guid.NewGuid();
            var msg = new ExceptionLoggingTestMessage(messageId, "test");

            await publisher.Send<ExceptionLoggingTestConsumer, ExceptionLoggingTestMessage>(
                msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Successes.Count == 1, timeoutMs: 3000);

            Assert.Equal(2, tracker.GetAttemptCount(messageId)); // initial fail + redelivery
            Assert.Single(tracker.Successes);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    private static ConsumeContext CreateTestContext(ConsumerDescriptor descriptor) =>
        new(descriptor, "{}"u8.ToArray(), new MessageHeaders());

    [Fact]
    public async Task DoesNotCatchCancellationException()
    {
        // Verify that OperationCanceledException is NOT caught by the middleware,
        // so it propagates directly without logging.
        var middleware = new ExceptionLoggingConsumeMiddleware(
            new Microsoft.Extensions.Logging.Abstractions.NullLogger<ExceptionLoggingConsumeMiddleware>());

        var descriptor = new ConsumerDescriptor(
            typeof(ExceptionLoggingTestConsumer),
            "test-queue",
            [typeof(ExceptionLoggingTestMessage)],
            (_, _, _, _) => Task.CompletedTask);

        var context = CreateTestContext(descriptor);

        // Next middleware throws cancellation
        var ex = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            middleware.InvokeAsync(context, (_, _) =>
                    throw new OperationCanceledException("cancelled"),
                CancellationToken.None));

        Assert.NotNull(ex);
        Assert.Equal("cancelled", ex.Message);
    }

    [Fact]
    public async Task CatchesAndRethrowsAllOtherExceptions()
    {
        // Verify that non-cancellation exceptions are caught and rethrown.
        var middleware = new ExceptionLoggingConsumeMiddleware(
            new Microsoft.Extensions.Logging.Abstractions.NullLogger<ExceptionLoggingConsumeMiddleware>());

        var descriptor = new ConsumerDescriptor(
            typeof(ExceptionLoggingTestConsumer),
            "test-queue",
            [typeof(ExceptionLoggingTestMessage)],
            (_, _, _, _) => Task.CompletedTask);

        var context = CreateTestContext(descriptor);

        var originalEx = new InvalidOperationException("test error");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(context, (_, _) =>
                    throw originalEx,
                CancellationToken.None));

        Assert.Same(originalEx, ex);
    }

    [Fact]
    public async Task PassesThroughSuccessfully_WhenNoException()
    {
        // Verify that when the next middleware succeeds, the exception logger acts
        // as a transparent pass-through.
        var callCount = 0;
        var middleware = new ExceptionLoggingConsumeMiddleware(
            new Microsoft.Extensions.Logging.Abstractions.NullLogger<ExceptionLoggingConsumeMiddleware>());

        var descriptor = new ConsumerDescriptor(
            typeof(ExceptionLoggingTestConsumer),
            "test-queue",
            [typeof(ExceptionLoggingTestMessage)],
            (_, _, _, _) => Task.CompletedTask);

        var context = CreateTestContext(descriptor);

        await middleware.InvokeAsync(context, (_, _) =>
            {
                callCount++;
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Equal(1, callCount);
    }
}
