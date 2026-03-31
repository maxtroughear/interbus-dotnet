using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Retry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

public sealed class RedeliveryRetryIntegrationTests
{
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

    [Fact]
    public async Task RedeliveryMiddleware_RedeliversFailedMessage_WithIncrementedHeader()
    {
        var tracker = new RedeliveryTracker();

        var (provider, cts) = await StartAsync(
            services => { services.AddSingleton(tracker); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<RedeliveryTestConsumer>();
                bus.AddScheduling();
                bus.UseRedeliveryImmediateMiddleware(options => options.RetryCount = 3);
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var message = new RedeliveryTestMessage(Guid.NewGuid());

            await publisher.Send<RedeliveryTestConsumer, RedeliveryTestMessage>(
                message,
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Successes.Contains(message.Id));

            Assert.Equal(2, tracker.GetAttemptCount(message.Id));
            Assert.Contains("1", tracker.SeenRetryHeaders);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}

public sealed record RedeliveryTestMessage(Guid Id);

public sealed class RedeliveryTracker
{
    private readonly ConcurrentDictionary<Guid, int> _attemptCounts = [];
    private readonly ConcurrentBag<Guid> _successes = [];
    private readonly ConcurrentBag<string?> _seenRetryHeaders = [];

    public IReadOnlyCollection<Guid> Successes => _successes;
    public IReadOnlyCollection<string?> SeenRetryHeaders => _seenRetryHeaders;

    public int GetAttemptCount(Guid messageId) => _attemptCounts.GetOrAdd(messageId, 0);

    public int RecordAttempt(Guid messageId)
    {
        var updated = _attemptCounts.AddOrUpdate(messageId, 1, (_, count) => count + 1);
        return updated;
    }

    public void RecordSuccess(Guid messageId) => _successes.Add(messageId);

    public void RecordRetryHeader(IReadOnlyDictionary<string, string> headers)
    {
        headers.TryGetValue(RedeliveryImmediateConsumeMiddleware.RetryCountHeader, out var value);
        _seenRetryHeaders.Add(value);
    }
}

public sealed class RedeliveryTestConsumer(RedeliveryTracker tracker) : IConsumer<RedeliveryTestMessage>
{
    public Task Consume(RedeliveryTestMessage message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        var attempt = tracker.RecordAttempt(message.Id);
        tracker.RecordRetryHeader(context.Headers);

        if (attempt == 1)
            throw new InvalidOperationException("fail first attempt");

        tracker.RecordSuccess(message.Id);
        return Task.CompletedTask;
    }
}
