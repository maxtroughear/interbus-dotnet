using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public class InMemorySchedulerTests
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
    public async Task SchedulePublishAsync_MessageIsDelivered_WhenScheduledTimeElapses()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddInMemoryScheduler();
                bus.AddScheduling(builder => builder.Directly());
                bus.AddConsumer<OrderPlacedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();

            var message = new OrderPlaced(Guid.NewGuid(), "scheduled@example.com", 42m);
            var scheduledAt = DateTimeOffset.UtcNow.AddMilliseconds(500);

            await scheduler.SchedulePublishAsync(message, scheduledAt, TestContext.Current.CancellationToken);

            await Task.Delay(150, TestContext.Current.CancellationToken);
            Assert.Empty(tracker.Messages);

            await WaitUntilAsync(() => tracker.Messages.Count == 1, timeoutMs: 4000);

            var delivered = Assert.Single(tracker.Messages);
            Assert.Equal(message.OrderId, delivered.OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task CancelAsync_ScheduledMessage_IsNotDelivered()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddInMemoryScheduler();
                bus.AddScheduling(builder => builder.Directly());
                bus.AddConsumer<OrderPlacedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();

            var message = new OrderPlaced(Guid.NewGuid(), "cancel@example.com", 10m);
            var scheduleId = await scheduler.SchedulePublishAsync(
                message,
                DateTimeOffset.UtcNow.AddMilliseconds(400),
                TestContext.Current.CancellationToken);

            Assert.NotNull(scheduleId);
            await scheduler.CancelAsync(scheduleId, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Messages.Count > 0, timeoutMs: 1600);
            Assert.Empty(tracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task ScheduleSendAsync_ByConsumerType_OnlyTargetQueueReceivesMessage()
    {
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(orderTracker);
                services.AddSingleton(auditTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddInMemoryScheduler();
                bus.AddScheduling(builder => builder.Directly());
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<AuditConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();

            var message = new OrderPlaced(Guid.NewGuid(), "queue-target@example.com", 11m);

            await scheduler.ScheduleSendAsync<OrderPlacedConsumer, OrderPlaced>(
                message,
                DateTimeOffset.UtcNow.AddMilliseconds(300),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1, timeoutMs: 4000);

            var delivered = Assert.Single(orderTracker.Messages);
            Assert.Equal(message.OrderId, delivered.OrderId);
            Assert.Empty(auditTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
