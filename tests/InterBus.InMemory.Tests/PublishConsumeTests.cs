using InterBus.Extensions;
using InterBus.Generated;
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
    public async Task Publish_SingleConsumer_MessageIsDelivered()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services => { services.AddSingleton(tracker); },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddGeneratedConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

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
                bus.AddGeneratedConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "fan@out.test", 50m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

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
                bus.AddGeneratedConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "direct@send.test", 25m);

            // Send directly to OrderPlacedConsumer only - AuditConsumer should NOT receive.
            await publisher.Send<OrderPlacedConsumer, OrderPlaced>(msg, TestContext.Current.CancellationToken);

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

    [Fact]
    public async Task AddGeneratedConsumers_RegistersAllDiscoveredConsumers_MessagesDelivered()
    {
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        // Use AddGeneratedConsumers() — the source generator discovered OrderPlacedConsumer and
        // AuditConsumer at compile time and emitted reflection-free dispatch delegates for each.
        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(orderTracker);
                services.AddSingleton(auditTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddGeneratedConsumers();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "generated@test.com", 75m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            // Both consumers are registered via AddGeneratedConsumers - both should receive.
            await WaitUntilAsync(() =>
                orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1);

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

    /// <summary>
    /// Verifies the one-queue-per-consumer design: a consumer implementing two <c>IConsumer&lt;T&gt;</c>
    /// interfaces shares a single queue. Both message types are dispatched to the correct
    /// <c>Consume</c> overload via the source-generated delegate — no runtime reflection.
    /// </summary>
    [Fact]
    public async Task MultiTypeConsumer_SingleQueue_ReceivesBothMessageTypes()
    {
        var orderTracker = new MultiTypeOrderPlacedTracker();
        var shippedTracker = new MultiTypeOrderShippedTracker();
        InterBusOptions? capturedOptions = null;

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(orderTracker);
                services.AddSingleton(shippedTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddGeneratedConsumers();
                capturedOptions = bus.Options;
            });

        try
        {
            // Structural assertion: ONE descriptor for MultiTypeConsumer with TWO topic names.
            var descriptor = capturedOptions!.Consumers
                .Single(c => c.ConsumerType == typeof(MultiTypeConsumer));
            Assert.Equal(2, descriptor.TopicNames.Count);

            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

            var orderMsg = new OrderPlaced(Guid.NewGuid(), "multi@test.com", 100m);
            var shippedMsg = new OrderShipped(Guid.NewGuid(), "TRK-001");

            await publisher.Publish(orderMsg, TestContext.Current.CancellationToken);
            await publisher.Publish(shippedMsg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() =>
                orderTracker.Messages.Count == 1 && shippedTracker.Messages.Count == 1);

            Assert.Single(orderTracker.Messages);
            Assert.Equal(orderMsg.OrderId, orderTracker.Messages.Single().OrderId);
            Assert.Single(shippedTracker.Messages);
            Assert.Equal(shippedMsg.OrderId, shippedTracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
