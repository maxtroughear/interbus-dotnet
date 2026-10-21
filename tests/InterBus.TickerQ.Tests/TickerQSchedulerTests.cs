using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.TickerQ.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TickerQ.DependencyInjection;
using TickerQ.Utilities;
using Xunit;

namespace InterBus.TickerQ.Tests;

/// <summary>
/// Unit tests for <see cref="TickerQMessageScheduler"/> and <see cref="ScheduledDeliveryRequest"/>.
/// </summary>
public sealed partial class TickerQSchedulerTests
{
    [Fact]
    public async Task SchedulePublishAsync_MessageIsConsumed_UsingTickerQInMemoryStorage()
    {
        var tracker = new IntegrationMessageTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(tracker);
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling(builder => builder.Directly().AddTickerQScheduler());
            bus.RegisterConsumer<IntegrationMessageConsumer>(c =>
                c.Consumes<IntegrationMessageConsumer, IntegrationMessage>());
        });
        services.AddTickerQ(options => options.MinPollingInterval = TimeSpan.FromMilliseconds(50));

        TickerFunctionProvider.Build();

        var provider = services.BuildServiceProvider();

        // Wire up TickerQ's internal scheduler notification chain (normally done by UseTickerQ on IHost)
        new MinimalHost(provider).UseTickerQ();

        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        try
        {
            using var scope = provider.CreateScope();
            var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();

            var message = new IntegrationMessage(Guid.NewGuid(), "hello-tickerq");
            await scheduler.SchedulePublishAsync(
                message, DateTimeOffset.UtcNow.AddSeconds(-1), cts.Token);

            await WaitUntilAsync(() => tracker.Messages.Count >= 1, timeoutMs: 5000, cancellationToken: TestContext.Current.CancellationToken);

            var delivered = Assert.Single(tracker.Messages);
            Assert.Equal(message.Id, delivered.Id);
            Assert.Equal(message.Payload, delivered.Payload);
        }
        finally
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
    }
}
