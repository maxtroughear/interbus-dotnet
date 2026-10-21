using InterBus.Generated;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public sealed class RedeliveryRetryIntegrationTests
{
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
                bus.WithConsumers(b => b.UseRedeliveryImmediateMiddleware(options => options.RetryCount = 3));
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var message = new RedeliveryTestMessage(Guid.NewGuid());

            await publisher.SendAsync<RedeliveryTestConsumer, RedeliveryTestMessage>(
                message,
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Successes.Contains(message.Id), cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(2, tracker.GetAttemptCount(message.Id));
            Assert.Contains("1", tracker.SeenRetryHeaders);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task RedeliveryTransportDelayMiddleware_RedeliversFailedMessage_WithIncrementedHeader()
    {
        var tracker = new RedeliveryTracker();

        var (provider, cts) = await StartAsync(
            services => { services.AddSingleton(tracker); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<RedeliveryDelayTestConsumer>();
                bus.WithConsumers(b => b.UseRedeliveryTransportDelayMiddleware(options => options.RetryCount = 3));
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var message = new RedeliveryTestMessage(Guid.NewGuid());

            await publisher.SendAsync<RedeliveryDelayTestConsumer, RedeliveryTestMessage>(
                message,
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Successes.Contains(message.Id), cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(2, tracker.GetAttemptCount(message.Id));
            Assert.Contains("1", tracker.SeenDelayRetryHeaders);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task RedeliveryTransportDelayMiddleware_WithDelay_RedeliversFailedMessage()
    {
        var tracker = new RedeliveryTracker();

        var (provider, cts) = await StartAsync(
            services => { services.AddSingleton(tracker); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<RedeliveryDelayTestConsumer>();
                bus.WithConsumers(b => b.UseRedeliveryTransportDelayMiddleware(options =>
                {
                    options.RetryCount = 3;
                    options.DelayProvider = _ => TimeSpan.FromMilliseconds(10);
                }));
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var message = new RedeliveryTestMessage(Guid.NewGuid());

            await publisher.SendAsync<RedeliveryDelayTestConsumer, RedeliveryTestMessage>(
                message,
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Successes.Contains(message.Id), timeoutMs: 2000, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(2, tracker.GetAttemptCount(message.Id));
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
