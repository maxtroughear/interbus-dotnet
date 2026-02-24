using System.Collections.Concurrent;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

public class PublishConsumeTests
{
    /// <summary>
    /// Polls <paramref name="condition"/> up to <paramref name="timeoutMs"/> milliseconds,
    /// checking every <paramref name="intervalMs"/> milliseconds. Avoids hard-coded sleeps.
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

    /// <summary>
    /// Builds a DI container with the given consumer registrations and bus configuration,
    /// starts all hosted services, and returns the provider + CTS for cleanup.
    /// </summary>
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

    [Fact]
    public async Task Publish_SingleConsumer_MessageIsDelivered()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(tracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<OrderPlacedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);
            await publisher.Publish(msg);

            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            Assert.Single(tracker.Messages);
            Assert.Equal(msg.OrderId, tracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Publish_MultipleConsumers_AllReceiveMessage()
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
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<AuditConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "fan@out.test", 50m);
            await publisher.Publish(msg);

            // Both consumers should receive the message via topic fan-out.
            await WaitUntilAsync(() => orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1);

            Assert.Single(orderTracker.Messages);
            Assert.Equal(msg.OrderId, orderTracker.Messages.Single().OrderId);
            Assert.Single(auditTracker.Messages);
            Assert.Equal(msg.OrderId, auditTracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Send_DirectToConsumer_OnlyTargetReceives()
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
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<AuditConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "direct@send.test", 25m);

            // Send directly to OrderPlacedConsumer only — AuditConsumer should NOT receive.
            await publisher.Send<OrderPlacedConsumer, OrderPlaced>(msg);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1);

            Assert.Single(orderTracker.Messages);
            Assert.Equal(msg.OrderId, orderTracker.Messages.Single().OrderId);
            Assert.Empty(auditTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
