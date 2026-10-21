using InterBus.Generated;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.Retry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

public partial class InMemoryOutboxTests
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
    public async Task Publish_WithOutbox_SendsToTransport()
    {
        var orderPlacedTracker = new OrderPlacedTracker();
        var orderShippedTracker = new OrderShippedTracker();

        var (provider, cts) = await StartAsync(services =>
        {
            services.AddSingleton(orderPlacedTracker);
            services.AddSingleton(orderShippedTracker);
        }, bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedShippingConsumer>(c => c.UseInMemoryOutbox());
            bus.AddConsumer<OrderShippedConsumer>();
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);
            await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderPlacedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderPlacedTracker.Messages);
            Assert.Equal(msg.OrderId, orderPlacedTracker.Messages.Single().OrderId);

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderShippedTracker.Messages);
            Assert.Equal(msg.OrderId, orderShippedTracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Publish_WithOutbox_WaitsUntilConsumerCompleted()
    {
        var orderPlacedTracker = new OrderPlacedTracker();
        var orderShippedTracker = new OrderShippedTracker();
        var delayedController = new DelayedConsumerController();

        var (provider, cts) = await StartAsync(services =>
        {
            services.AddSingleton(orderPlacedTracker);
            services.AddSingleton(orderShippedTracker);
            services.AddSingleton(delayedController);
        }, bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedDelayedShippingConsumer>(c => c.UseInMemoryOutbox());
            bus.AddConsumer<OrderShippedConsumer>();
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);

            await delayedController.Semaphore.WaitAsync(TestContext.Current.CancellationToken);

            await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderPlacedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderPlacedTracker.Messages);
            Assert.Equal(msg.OrderId, orderPlacedTracker.Messages.Single().OrderId);

            // Wait a little bit to check the OrderShipped message is not published yet
            await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

            Assert.True(delayedController.Published);
            Assert.Empty(orderShippedTracker.Messages);

            delayedController.Semaphore.Release();

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderShippedTracker.Messages);
            Assert.Equal(msg.OrderId, orderShippedTracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Publish_WithOutbox_RetriesDontProduceDuplicateMessages()
    {
        var orderPlacedTracker = new OrderPlacedTracker();
        var orderShippedTracker = new OrderShippedTracker();
        var delayedController = new DelayedConsumerController();

        var (provider, cts) = await StartAsync(services =>
        {
            services.AddSingleton(orderPlacedTracker);
            services.AddSingleton(orderShippedTracker);
            services.AddSingleton(delayedController);
        }, bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedDelayedShippingConsumer>(c =>
            {
                c.UseRetryMiddleware(options =>
                {
                    options.RetryCount = 2;
                    options.DelayProvider = RetryDelays.Constant(TimeSpan.FromSeconds(1));
                });
                c.UseInMemoryOutbox();
            });
            bus.AddConsumer<OrderShippedConsumer>();
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);

            await delayedController.Semaphore.WaitAsync(TestContext.Current.CancellationToken);

            await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderPlacedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderPlacedTracker.Messages);
            Assert.Equal(msg.OrderId, orderPlacedTracker.Messages.Single().OrderId);

            await WaitUntilAsync(() => delayedController.Published, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Empty(orderShippedTracker.Messages);

            delayedController.ShouldFail = true;
            delayedController.Semaphore.Release();

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderShippedTracker.Messages);
            Assert.Equal(msg.OrderId, orderShippedTracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Publish_WithOutbox_ConsumerOnlyRunsSuccessfullyOnce()
    {
        var orderPlacedTracker = new OrderPlacedTracker();
        var orderShippedTracker = new OrderShippedTracker();
        var delayedController = new DelayedConsumerController();

        var (provider, cts) = await StartAsync(services =>
        {
            services.AddSingleton(orderPlacedTracker);
            services.AddSingleton(orderShippedTracker);
            services.AddSingleton(delayedController);
        }, bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedDelayedShippingConsumer>(c =>
            {
                c.UseRetryMiddleware(options =>
                {
                    options.RetryCount = 2;
                    options.DelayProvider = RetryDelays.Constant(TimeSpan.FromSeconds(1));
                });
                c.UseInMemoryOutbox();
                c.UseSendMiddleware<SimulateFailure>();
            });
            bus.AddConsumer<OrderShippedConsumer>();
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);

            await delayedController.Semaphore.WaitAsync(TestContext.Current.CancellationToken);

            await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderPlacedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderPlacedTracker.Messages);
            Assert.Equal(msg.OrderId, orderPlacedTracker.Messages.Single().OrderId);

            await WaitUntilAsync(() => delayedController.Published, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Empty(orderShippedTracker.Messages);

            delayedController.Semaphore.Release();

            await WaitUntilAsync(() => orderShippedTracker.Messages.Count == 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(orderPlacedTracker.Messages);
            Assert.Single(orderShippedTracker.Messages);
            Assert.Equal(msg.OrderId, orderShippedTracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
