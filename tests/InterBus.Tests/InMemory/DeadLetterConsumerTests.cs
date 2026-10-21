using InterBus.Generated;
using InterBus.DeadLettering;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

/// <summary>
/// Integration tests verifying that dead-lettered messages are published as
/// <see cref="DeadLetter{T}"/> to the appropriate topic so application consumers
/// can subscribe and process them.
/// </summary>
public partial class DeadLetterConsumerTests
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
    public async Task DeadLetteredMessage_IsPublishedToDeadLetterTopic_AndReceivedByConsumer()
    {
        var faultingCount = new FaultingConsumerCallCount();
        var deadLetterTracker = new DeadLetterOrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(faultingCount);
                services.AddSingleton(deadLetterTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<FaultingOrderPlacedConsumer>();
                bus.AddConsumer<DeadLetterOrderPlacedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(
                new OrderPlaced(Guid.NewGuid(), "deadletter@test.com", 99m),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => deadLetterTracker.Messages.Count >= 1, cancellationToken: TestContext.Current.CancellationToken);

            Assert.NotEmpty(deadLetterTracker.Messages);
            var deadLetter = deadLetterTracker.Messages.First();
            Assert.NotNull(deadLetter.OriginalMessage);
            Assert.Equal("deadletter@test.com", deadLetter.OriginalMessage.CustomerEmail);
            Assert.Equal("InvalidOperationException", deadLetter.Reason);
            Assert.Contains("Intentional failure for dead-letter test", deadLetter.Error);
            Assert.True(deadLetter.DeadLetteredAtUtc <= DateTimeOffset.UtcNow);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task DeadLetterConsumer_WhenItFails_DoesNotPublishNestedDeadLetter()
    {
        // Arrange: a consumer of DeadLetter<OrderPlaced> that itself throws.
        // It should NOT publish a DeadLetter<DeadLetter<OrderPlaced>>.
        var faultingCount = new FaultingConsumerCallCount();
        var nestedDeadLetterTracker = new NestedDeadLetterTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(faultingCount);
                services.AddSingleton(nestedDeadLetterTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<FaultingOrderPlacedConsumer>();
                bus.AddConsumer<FaultingDeadLetterConsumer>();
                bus.AddConsumer<NestedDeadLetterConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(
                new OrderPlaced(Guid.NewGuid(), "nested@test.com", 1m),
                TestContext.Current.CancellationToken);

            // Give enough time for dead letter to flow through and verify no nested dead letter is produced.
            await WaitUntilAsync(() => faultingCount.Count >= 1, cancellationToken: TestContext.Current.CancellationToken);
            await Task.Delay(200, TestContext.Current.CancellationToken);

            Assert.Empty(nestedDeadLetterTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task DeadLetteredMessage_IsNotRedelivered_AfterDeadLettering()
    {
        // Arrange: a consumer that always throws so every message is dead-lettered.
        // The pump must discard the message after dead-lettering rather than re-enqueuing it,
        // so the consumer should only be called exactly once per published message.
        var faultingCount = new FaultingConsumerCallCount();
        var deadLetterTracker = new DeadLetterOrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(faultingCount);
                services.AddSingleton(deadLetterTracker);
            },
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddConsumer<FaultingOrderPlacedConsumer>();
                bus.AddConsumer<DeadLetterOrderPlacedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(
                new OrderPlaced(Guid.NewGuid(), "loop@test.com", 10m),
                TestContext.Current.CancellationToken);

            // Wait until the dead letter is received, then allow extra time to confirm no redelivery.
            await WaitUntilAsync(() => deadLetterTracker.Messages.Count >= 1, cancellationToken: TestContext.Current.CancellationToken);
            await Task.Delay(200, TestContext.Current.CancellationToken);

            // The faulting consumer should have been called exactly once — no infinite loop.
            Assert.Equal(1, faultingCount.Count);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
