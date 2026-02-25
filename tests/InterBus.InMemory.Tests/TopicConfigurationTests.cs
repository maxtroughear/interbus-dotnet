using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

public class TopicConfigurationTests
{
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

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000, int intervalMs = 25)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    [Fact]
    public async Task ConfigureTopic_CustomTopicName_ConsumerReceivesPublishedMessage()
    {
        // Arrange: configure a custom topic name before registering the consumer.
        var tracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.ConfigureTopic<OrderPlaced>("custom.orders.topic");
                bus.AddConsumer<OrderPlacedConsumer, OrderPlaced>();
            });

        try
        {
            // Act: publish using the same configured topic name (resolved from options).
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "custom@topic.test", 10m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            // Assert: consumer received the message via the custom topic.
            Assert.Single(tracker.Messages);
            Assert.Equal(msg.OrderId, tracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public void ConfigureTopic_CustomTopicName_StoredInConsumerDescriptor()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddLogging();
        InterBusOptions? capturedOptions = null;

        services.AddInterBus(bus =>
        {
            bus.ConfigureTopic<OrderPlaced>("my-orders");
            bus.AddConsumer<OrderPlacedConsumer, OrderPlaced>();
            capturedOptions = bus.Options;
        });

        // Assert: the descriptor's TopicName reflects the configured value.
        Assert.NotNull(capturedOptions);
        var descriptor = capturedOptions.Consumers.Single(c => c.ConsumerType == typeof(OrderPlacedConsumer));
        Assert.Equal("my-orders", descriptor.TopicName);
    }

    [Fact]
    public void ConfigureTopic_MultipleConsumers_SameTopicNameUsedForAll()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        InterBusOptions? capturedOptions = null;

        services.AddInterBus(bus =>
        {
            bus.ConfigureTopic<OrderPlaced>("shared-orders");
            bus.AddConsumer<OrderPlacedConsumer, OrderPlaced>();
            bus.AddConsumer<AuditConsumer, OrderPlaced>();
            capturedOptions = bus.Options;
        });

        Assert.NotNull(capturedOptions);
        var descriptors = capturedOptions.Consumers
            .Where(c => c.MessageType == typeof(OrderPlaced))
            .ToList();

        Assert.Equal(2, descriptors.Count);
        Assert.All(descriptors, d => Assert.Equal("shared-orders", d.TopicName));
    }

    [Fact]
    public async Task ConfigureTopic_CustomTopicName_MultipleConsumersAllReceiveMessage()
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
                bus.ConfigureTopic<OrderPlaced>("orders");
                bus.AddConsumer<OrderPlacedConsumer, OrderPlaced>();
                bus.AddConsumer<AuditConsumer, OrderPlaced>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "fanout@custom.test", 20m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1);

            Assert.Single(orderTracker.Messages);
            Assert.Single(auditTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Send_ResolvesQueueFromRegisteredDescriptor_OnlyTargetConsumerReceives()
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
                bus.ConfigureTopic<OrderPlaced>("orders");
                bus.AddConsumer<OrderPlacedConsumer, OrderPlaced>();
                bus.AddConsumer<AuditConsumer, OrderPlaced>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "send@resolve.test", 30m);

            // Send directly to OrderPlacedConsumer - queue is resolved from the registered descriptor.
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
}
