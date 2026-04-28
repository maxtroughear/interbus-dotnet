using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.InMemory.Tests;

// ---------------------------------------------------------------------------
// Test messages (top-level to avoid nested-type name mismatch between CLR and
// source-generator display strings, e.g. "A+B" vs "A.B")
// ---------------------------------------------------------------------------

public record OrderStarted(Guid OrderId, string CustomerEmail);
public record OrderConfirmed(Guid OrderId);
public record OrderCancelled(Guid OrderId);
public record OrderAcknowledged(Guid OrderId);

// ---------------------------------------------------------------------------
// Test saga state
// ---------------------------------------------------------------------------

public sealed class OrderSagaState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public bool IsCancelled { get; set; }
    public int MessageCount { get; set; }
}

// ---------------------------------------------------------------------------
// Simple saga: handles OrderStarted and OrderConfirmed
// ---------------------------------------------------------------------------

public sealed class OrderSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderConfirmed>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderConfirmed message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}

// ---------------------------------------------------------------------------
// Saga with deletion: handles OrderStarted and OrderCancelled (delete on cancel)
// ---------------------------------------------------------------------------

public sealed class OrderWithDeletionSaga :
    ISagaConsumer<OrderSagaState, OrderStarted>,
    ISagaConsumer<OrderSagaState, OrderCancelled>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(OrderCancelled message) => message.OrderId.ToString();

    public Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(OrderCancelled message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsCancelled = true;
        context.Delete();
        return Task.CompletedTask;
    }
}

// ---------------------------------------------------------------------------
// Saga that publishes a message from within Consume
// ---------------------------------------------------------------------------

public sealed class AcknowledgingOrderSaga(IPublisher publisher) :
    ISagaConsumer<OrderSagaState, OrderStarted>
{
    public string GetCorrelationId(OrderStarted message) => message.OrderId.ToString();

    public async Task Consume(OrderStarted message, ISagaContext<OrderSagaState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        await publisher.PublishAsync(new OrderAcknowledged(message.OrderId), cancellationToken);
    }
}

public sealed class OrderAcknowledgedTracker
{
    private readonly System.Collections.Concurrent.ConcurrentBag<OrderAcknowledged> _messages = [];
    public IReadOnlyCollection<OrderAcknowledged> Messages => _messages;
    public void Track(OrderAcknowledged msg) => _messages.Add(msg);
}

public sealed class OrderAcknowledgedConsumer(OrderAcknowledgedTracker tracker) :
    IConsumer<OrderAcknowledged>
{
    public Task Consume(OrderAcknowledged message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Integration tests for saga functionality: state management, correlation, concurrency locking,
/// TTL, explicit deletion, and publishing from within a saga.
/// </summary>
public class SagaTests
{
    // ---------------------------------------------------------------------------
    // Helper infrastructure
    // ---------------------------------------------------------------------------

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
        Action<IServiceCollection>? registerSingletons,
        Action<IInterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        registerSingletons?.Invoke(services);
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

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task Saga_SingleMessage_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartAsync(
            null,
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<OrderSaga, OrderSagaState>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "test@example.com"));

            // Allow the saga to process
            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();
                var state = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return state?.MessageCount == 1;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();
            var sagaState = await repository.GetAsync(orderId.ToString());

            Assert.NotNull(sagaState);
            Assert.Equal("test@example.com", sagaState.CustomerEmail);
            Assert.Equal(1, sagaState.MessageCount);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_MultipleMessages_SameCorrelationId_StateAccumulates()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartAsync(
            null,
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<OrderSaga, OrderSagaState>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "order@test.com"));
            await publisher.PublishAsync(new OrderConfirmed(orderId));

            // Wait for both messages to be processed
            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();
                var state = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return state?.MessageCount == 2;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();
            var sagaState = await repository.GetAsync(orderId.ToString());

            Assert.NotNull(sagaState);
            Assert.Equal("order@test.com", sagaState.CustomerEmail);
            Assert.True(sagaState.IsConfirmed);
            Assert.Equal(2, sagaState.MessageCount);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_DifferentCorrelationIds_MaintainSeparateState()
    {
        var orderId1 = Guid.NewGuid();
        var orderId2 = Guid.NewGuid();

        var (provider, cts) = await StartAsync(
            null,
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<OrderSaga, OrderSagaState>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId1, "first@test.com"));
            await publisher.PublishAsync(new OrderStarted(orderId2, "second@test.com"));

            var repository = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();

            await WaitUntilAsync(() =>
            {
                var s1 = repository.GetAsync(orderId1.ToString()).GetAwaiter().GetResult();
                var s2 = repository.GetAsync(orderId2.ToString()).GetAwaiter().GetResult();
                return s1?.MessageCount == 1 && s2?.MessageCount == 1;
            });

            var state1 = await repository.GetAsync(orderId1.ToString());
            var state2 = await repository.GetAsync(orderId2.ToString());

            Assert.NotNull(state1);
            Assert.NotNull(state2);
            Assert.Equal("first@test.com", state1.CustomerEmail);
            Assert.Equal("second@test.com", state2.CustomerEmail);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_Delete_RemovesStateFromRepository()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartAsync(
            null,
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<OrderWithDeletionSaga, OrderSagaState>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "cancel@test.com"));

            var repository = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();

            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.MessageCount == 1);

            // Now cancel the order - saga should call Delete()
            await publisher.PublishAsync(new OrderCancelled(orderId));

            // Wait for the saga to process the cancellation and delete its state
            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult() == null,
                timeoutMs: 3000);

            var state = await repository.GetAsync(orderId.ToString());
            Assert.Null(state);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_WithTtl_StateExpiresAfterTtl()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartAsync(
            null,
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<OrderSaga, OrderSagaState>(saga =>
                    saga.WithTimeToLive(TimeSpan.FromMilliseconds(200)));
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "ttl@test.com"));

            var repository = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();

            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.MessageCount == 1);

            // State should be present immediately after processing
            var stateBeforeExpiry = await repository.GetAsync(orderId.ToString());
            Assert.NotNull(stateBeforeExpiry);

            // Wait for TTL to expire
            await Task.Delay(300);

            // State should now be expired (lazy removal on next read)
            var stateAfterExpiry = await repository.GetAsync(orderId.ToString());
            Assert.Null(stateAfterExpiry);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_PublishesMessageFromConsume_MessageIsDelivered()
    {
        var orderId = Guid.NewGuid();
        var tracker = new OrderAcknowledgedTracker();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<AcknowledgingOrderSaga, OrderSagaState>();
                bus.AddConsumer<OrderAcknowledgedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "publish@test.com"));

            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            Assert.Single(tracker.Messages);
            Assert.Equal(orderId, tracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_NewInstance_StateStartsEmpty()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartAsync(
            null,
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<OrderSaga, OrderSagaState>();
            });

        try
        {
            // Send OrderConfirmed first (before OrderStarted) - a new saga state should be created
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderConfirmed(orderId));

            var repository = provider.GetRequiredService<ISagaRepository<OrderSagaState>>();

            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.MessageCount == 1);

            var state = await repository.GetAsync(orderId.ToString());
            Assert.NotNull(state);
            Assert.True(state.IsConfirmed);
            Assert.Null(state.CustomerEmail); // CustomerEmail not set because OrderStarted was not sent
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
