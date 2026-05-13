using InterBus.DeadLettering;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

/// <summary>
/// Integration tests verifying that dead-lettered messages are published as
/// <see cref="DeadLetter{T}"/> to the appropriate topic so application consumers
/// can subscribe and process them.
/// </summary>
public class DeadLetterConsumerTests
{
    private static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 5000,
        int intervalMs = 25)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

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

            await WaitUntilAsync(() => deadLetterTracker.Messages.Count >= 1);

            Assert.NotEmpty(deadLetterTracker.Messages);
            var deadLetter = deadLetterTracker.Messages.First();
            Assert.NotNull(deadLetter.OriginalMessage);
            Assert.Equal("deadletter@test.com", deadLetter.OriginalMessage.CustomerEmail);
            Assert.Equal("Unhandled exception in consume pipeline", deadLetter.Reason);
            Assert.NotEmpty(deadLetter.Error);
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
            await WaitUntilAsync(() => faultingCount.Count >= 1);
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
            await WaitUntilAsync(() => deadLetterTracker.Messages.Count >= 1);
            await Task.Delay(200, TestContext.Current.CancellationToken);

            // The faulting consumer should have been called exactly once — no infinite loop.
            Assert.Equal(1, faultingCount.Count);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    // ---- Support types -------------------------------------------------------

    public sealed class FaultingConsumerCallCount
    {
        private int _count;
        public int Count => _count;
        public void Increment() => Interlocked.Increment(ref _count);
    }

    /// <summary>Consumer that always throws, causing the message to be dead-lettered.</summary>
    public sealed class FaultingOrderPlacedConsumer(FaultingConsumerCallCount count) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            count.Increment();
            throw new InvalidOperationException("Intentional failure for dead-letter test");
        }
    }

    /// <summary>Consumer of <see cref="DeadLetter{T}"/> that itself always throws.</summary>
    public sealed class FaultingDeadLetterConsumer(FaultingConsumerCallCount count)
        : IConsumer<DeadLetter<OrderPlaced>>
    {
        public Task Consume(DeadLetter<OrderPlaced> message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            count.Increment();
            throw new InvalidOperationException("Intentional dead-letter consumer failure");
        }
    }

    /// <summary>Tracks nested dead letters (should never be received).</summary>
    public sealed class NestedDeadLetterTracker : MessageTracker<DeadLetter<DeadLetter<OrderPlaced>>>;

    /// <summary>Consumer of <c>DeadLetter&lt;DeadLetter&lt;OrderPlaced&gt;&gt;</c> - should never be invoked.</summary>
    public sealed class NestedDeadLetterConsumer(NestedDeadLetterTracker tracker)
        : IConsumer<DeadLetter<DeadLetter<OrderPlaced>>>
    {
        public Task Consume(DeadLetter<DeadLetter<OrderPlaced>> message, IConsumeContext context,
            CancellationToken cancellationToken = default)
        {
            tracker.Track(message);
            return Task.CompletedTask;
        }
    }
}
