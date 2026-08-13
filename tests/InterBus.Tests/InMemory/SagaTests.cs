using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.Tests.InMemory;

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
// Saga with completion: handles OrderStarted and OrderConfirmed (complete on confirm)
// ---------------------------------------------------------------------------

public sealed class CompletingOrderSaga :
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
        context.State.MessageCount++;
        context.Complete();
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
/// TTL, explicit deletion, completion, and publishing from within a saga.
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
        Action<InterBusBuilder> configureBus)
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
                bus.AddSaga<OrderSaga, OrderSagaState>(saga => saga.UseInMemoryRepository());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "test@example.com"),
                TestContext.Current.CancellationToken);

            // Allow the saga to process
            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
                var entry = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return entry?.State.MessageCount == 1;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
            var sagaEntry = await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);

            Assert.NotNull(sagaEntry);
            Assert.Equal("test@example.com", sagaEntry.State.CustomerEmail);
            Assert.Equal(1, sagaEntry.State.MessageCount);
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
                bus.AddSaga<OrderSaga, OrderSagaState>(saga => saga.UseInMemoryRepository());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "order@test.com"),
                TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new OrderConfirmed(orderId), TestContext.Current.CancellationToken);

            // Wait for both messages to be processed
            await WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
                var entry = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return entry?.State.MessageCount == 2;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
            var sagaEntry = await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);

            Assert.NotNull(sagaEntry);
            Assert.Equal("order@test.com", sagaEntry.State.CustomerEmail);
            Assert.True(sagaEntry.State.IsConfirmed);
            Assert.Equal(2, sagaEntry.State.MessageCount);
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
                bus.AddSaga<OrderSaga, OrderSagaState>(saga => saga.UseInMemoryRepository());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId1, "first@test.com"),
                TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new OrderStarted(orderId2, "second@test.com"),
                TestContext.Current.CancellationToken);

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();

            await WaitUntilAsync(() =>
            {
                var s1 = repository.GetAsync(orderId1.ToString()).GetAwaiter().GetResult();
                var s2 = repository.GetAsync(orderId2.ToString()).GetAwaiter().GetResult();
                return s1?.State.MessageCount == 1 && s2?.State.MessageCount == 1;
            });

            var entry1 = await repository.GetAsync(orderId1.ToString(), TestContext.Current.CancellationToken);
            var entry2 = await repository.GetAsync(orderId2.ToString(), TestContext.Current.CancellationToken);

            Assert.NotNull(entry1);
            Assert.NotNull(entry2);
            Assert.Equal("first@test.com", entry1.State.CustomerEmail);
            Assert.Equal("second@test.com", entry2.State.CustomerEmail);
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
                bus.AddSaga<OrderWithDeletionSaga, OrderSagaState>(saga => saga.UseInMemoryRepository());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "cancel@test.com"),
                TestContext.Current.CancellationToken);

            var repository = provider.GetRequiredService<ISagaRepository<OrderWithDeletionSaga, OrderSagaState>>();

            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.MessageCount == 1);

            // Now cancel the order - saga should call Delete()
            await publisher.PublishAsync(new OrderCancelled(orderId), TestContext.Current.CancellationToken);

            // Wait for the saga to process the cancellation and delete its state
            await WaitUntilAsync(() =>
                    repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult() == null,
                timeoutMs: 3000);

            var entry = await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);
            Assert.Null(entry);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_Complete_PreservesStateAndStopsProcessingNewMessages()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartAsync(
            null,
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<CompletingOrderSaga, OrderSagaState>(saga => saga.UseInMemoryRepository());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var repository = provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>();

            await publisher.PublishAsync(new OrderStarted(orderId, "complete@test.com"),
                TestContext.Current.CancellationToken);
            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.MessageCount == 1);

            // Confirming completes the saga
            await publisher.PublishAsync(new OrderConfirmed(orderId), TestContext.Current.CancellationToken);
            await WaitUntilAsync(() =>
            {
                var e = repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return e?.IsCompleted == true;
            });

            // State is preserved and marked completed
            var completedEntry = await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);
            Assert.NotNull(completedEntry);
            Assert.True(completedEntry.IsCompleted);
            Assert.Equal("complete@test.com", completedEntry.State.CustomerEmail);
            Assert.Equal(2, completedEntry.State.MessageCount);

            // Send another message - should be dropped because the saga is completed.
            // Wait briefly to give the message pump an opportunity to (incorrectly) process it.
            await publisher.PublishAsync(new OrderStarted(orderId, "new@test.com"),
                TestContext.Current.CancellationToken);
            await WaitUntilAsync(
                () => repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()
                    ?.State.CustomerEmail == "new@test.com",
                timeoutMs: 500, intervalMs: 10);

            // State must be unchanged — the message was silently dropped
            var entryAfter = await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);
            Assert.NotNull(entryAfter);
            Assert.True(entryAfter.IsCompleted);
            Assert.Equal("complete@test.com", entryAfter.State.CustomerEmail);
            Assert.Equal(2, entryAfter.State.MessageCount);
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
                    saga.UseInMemoryRepository().WithTimeToLive(TimeSpan.FromMilliseconds(200)));
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "ttl@test.com"),
                TestContext.Current.CancellationToken);

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();

            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.MessageCount == 1);

            // State should be present immediately after processing
            var entryBeforeExpiry =
                await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);
            Assert.NotNull(entryBeforeExpiry);

            // Wait for TTL to expire
            await Task.Delay(300, TestContext.Current.CancellationToken);

            // State should now be expired (lazy removal on next read)
            var entryAfterExpiry = await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);
            Assert.Null(entryAfterExpiry);
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
                bus.AddSaga<AcknowledgingOrderSaga, OrderSagaState>(saga => saga.UseInMemoryRepository());
                bus.AddConsumer<OrderAcknowledgedConsumer>();
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new OrderStarted(orderId, "publish@test.com"),
                TestContext.Current.CancellationToken);

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
                bus.AddSaga<OrderSaga, OrderSagaState>(saga => saga.UseInMemoryRepository());
            });

        try
        {
            // Send OrderConfirmed first (before OrderStarted) - a new saga state should be created
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderConfirmed(orderId), TestContext.Current.CancellationToken);

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();

            await WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.MessageCount == 1);

            var sagaEntry = await repository.GetAsync(orderId.ToString(), TestContext.Current.CancellationToken);
            Assert.NotNull(sagaEntry);
            Assert.True(sagaEntry.State.IsConfirmed);
            Assert.Null(sagaEntry.State.CustomerEmail); // CustomerEmail not set because OrderStarted was not sent
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Saga_SagaCorrelationId_MatchesCorrelationId()
    {
        var orderId = Guid.NewGuid();
        var capture = new SagaCorrelationCapture();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(capture),
            bus =>
            {
                bus.AddInMemoryTransport();
                bus.AddSaga<SagaCorrelationIdCapturingSaga, OrderSagaState>(saga =>
                    saga.UseInMemoryRepository());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "test@example.com"),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => capture.CapturedSagaCorrelationId is not null);

            Assert.Equal(orderId.ToString(), capture.CapturedSagaCorrelationId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
