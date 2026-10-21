using Hangfire;
using InterBus.Extensions;
using InterBus.Hangfire.Extensions;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Hangfire.Tests;

/// <summary>
/// Unit tests for <see cref="HangfireMessageScheduler"/> and <see cref="ScheduledDeliveryRequest"/>.
/// </summary>
public sealed partial class HangfireSchedulerTests
{
    [Fact]
    public async Task SchedulePublishAsync_MessageIsConsumed_UsingHangfireInMemoryStorage()
    {
        var tracker = new IntegrationMessageTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHangfire(c => c.UseInMemoryStorage());
        services.AddHangfireServer(c =>
        {
            c.WorkerCount = 1;
            c.SchedulePollingInterval = TimeSpan.FromMilliseconds(100);
        });
        services.AddSingleton(tracker);
        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddScheduling(builder => builder.Directly().AddHangfireScheduler());
            bus.RegisterConsumer<IntegrationMessageConsumer>(c =>
                c.Consumes<IntegrationMessageConsumer, IntegrationMessage>());
        });

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        try
        {
            using var scope = provider.CreateScope();
            var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();

            var message = new IntegrationMessage(Guid.NewGuid(), "hello-hangfire");
            await scheduler.SchedulePublishAsync(
                message, DateTimeOffset.UtcNow.AddSeconds(1), cts.Token);

            await WaitUntilAsync(() => tracker.Messages.Count >= 1, timeoutMs: 5000, cancellationToken: cts.Token);

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
