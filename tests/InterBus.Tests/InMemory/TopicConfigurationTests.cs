using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public class TopicConfigurationTests
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
    public async Task Configure_CustomTopicName_ConsumerReceivesPublishedMessage()
    {
        // Arrange: configure a custom topic name before registering the consumer.
        var tracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.Configure<OrderPlaced>(c => c.TopicName("custom.orders.topic"));
                bus.AddConsumer<OrderPlacedConsumer>();
            });

        try
        {
            // Act: publish using the same configured topic name (resolved from options).
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "custom@topic.test", 10m);
            await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Messages.Count == 1,
                cancellationToken: TestContext.Current.CancellationToken);

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
    public async Task Configure_CustomTopicName_MultipleConsumersAllReceiveMessage()
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
                bus.Configure<OrderPlaced>(c => c.TopicName("orders"));
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<AuditConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "fanout@custom.test", 20m);
            await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1,
                cancellationToken: TestContext.Current.CancellationToken);

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
                bus.Configure<OrderPlaced>(c => c.TopicName("orders"));
                bus.AddConsumer<OrderPlacedConsumer>();
                bus.AddConsumer<AuditConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "send@resolve.test", 30m);

            // Send directly to OrderPlacedConsumer - queue is resolved from the registered descriptor.
            await publisher.SendAsync<OrderPlacedConsumer, OrderPlaced>(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1,
                cancellationToken: TestContext.Current.CancellationToken);

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
    public async Task Send_ByQueueName_OnlyTargetConsumerReceives()
    {
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        InterBusOptions? capturedOptions = null;

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
                capturedOptions = bus.Options;
            });

        try
        {
            // Obtain the queue name from the descriptor (simulates a cross-project sender that resolves
            // the name once at startup and passes it as a string).
            var queueName = capturedOptions!.Consumers
                .Single(c => c.ConsumerType == typeof(OrderPlacedConsumer))
                .Queue;

            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "queue@name.test", 40m);

            await publisher.SendAsync(queueName, msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1,
                cancellationToken: TestContext.Current.CancellationToken);

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
    public async Task Send_ByQueueNameWithHeaders_ConsumerReceivesHeaders()
    {
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        InterBusOptions? capturedOptions = null;

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
                capturedOptions = bus.Options;
            });

        try
        {
            var queueName = capturedOptions!.Consumers
                .Single(c => c.ConsumerType == typeof(OrderPlacedConsumer))
                .Queue;

            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "hdr@queue.test", 5m);

            await publisher.SendAsync(queueName, msg, ctx => ctx.Headers["x-source"] = "cross-project",
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1,
                cancellationToken: TestContext.Current.CancellationToken);

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
