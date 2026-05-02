using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Sagas;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.EntityFrameworkCore.Tests;

// ---------------------------------------------------------------------------
// Saga-specific messages (separate from outbox test messages to avoid conflicts)
// ---------------------------------------------------------------------------

public record EfSagaOrderStarted(Guid OrderId, string CustomerEmail);
public record EfSagaOrderConfirmed(Guid OrderId);
public record EfSagaOrderCancelled(Guid OrderId);

// ---------------------------------------------------------------------------
// Saga state
// ---------------------------------------------------------------------------

public sealed class EfSagaOrderState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public bool IsCancelled { get; set; }
    public int MessageCount { get; set; }
}

// ---------------------------------------------------------------------------
// Saga implementations
// ---------------------------------------------------------------------------

public sealed class EfSagaOrderSaga :
    ISagaConsumer<EfSagaOrderState, EfSagaOrderStarted>,
    ISagaConsumer<EfSagaOrderState, EfSagaOrderConfirmed>
{
    public string GetCorrelationId(EfSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(EfSagaOrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(EfSagaOrderStarted message, ISagaContext<EfSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(EfSagaOrderConfirmed message, ISagaContext<EfSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}

public sealed class EfSagaOrderWithDeletionSaga :
    ISagaConsumer<EfSagaOrderState, EfSagaOrderStarted>,
    ISagaConsumer<EfSagaOrderState, EfSagaOrderCancelled>
{
    public string GetCorrelationId(EfSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(EfSagaOrderCancelled message) => message.OrderId.ToString();

    public Task Consume(EfSagaOrderStarted message, ISagaContext<EfSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(EfSagaOrderCancelled message, ISagaContext<EfSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsCancelled = true;
        context.Delete();
        return Task.CompletedTask;
    }
}

public sealed class EfSagaCompletingOrderSaga :
    ISagaConsumer<EfSagaOrderState, EfSagaOrderStarted>,
    ISagaConsumer<EfSagaOrderState, EfSagaOrderConfirmed>
{
    public string GetCorrelationId(EfSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(EfSagaOrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(EfSagaOrderStarted message, ISagaContext<EfSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(EfSagaOrderConfirmed message, ISagaContext<EfSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.MessageCount++;
        context.Complete();
        return Task.CompletedTask;
    }
}

// ---------------------------------------------------------------------------
// DbContext for saga tests
// ---------------------------------------------------------------------------

public sealed class SagaTestDbContext(DbContextOptions<SagaTestDbContext> options)
    : DbContext(options)
{
    public DbSet<SagaStateEntry> SagaStateEntries => Set<SagaStateEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddInterBusSagas();
}

/// <summary>
/// Integration tests for the EF Core saga repository using SQLite in-memory.
/// Verifies state persistence, multi-message accumulation, deletion, completion, and TTL
/// with the default in-process lock provider.
/// </summary>
public class EfCoreSagaTests
{
    // Async WaitUntil that creates a fresh scope per poll to work with scoped DbContext.
    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition,
        int timeoutMs = 5000,
        int intervalMs = 25)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!await condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts, SqliteConnection Connection)>
        StartAsync(Action<IInterBusBuilder> configureBus)
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<SagaTestDbContext>(o => o.UseSqlite(connection));
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<SagaTestDbContext>()
                .Database.EnsureCreatedAsync();
        }

        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts, connection);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts,
        SqliteConnection connection)
    {
        await cts.CancelAsync();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
        await connection.DisposeAsync();
    }

    // Helper to load a saga entry in its own scope (needed because DbContext is scoped).
    private static async Task<SagaEntry<EfSagaOrderState>?> GetSagaEntryAsync<TSaga>(
        ServiceProvider provider, string correlationId)
        where TSaga : class, ISaga<EfSagaOrderState>
    {
        await using var scope = provider.CreateAsyncScope();
        var repo = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<TSaga, EfSagaOrderState>>();
        return await repo.GetAsync(correlationId);
    }

    [Fact]
    public async Task Saga_SingleMessage_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "test@example.com"));

            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString()))
                    ?.State.MessageCount == 1);

            var entry = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString());

            Assert.NotNull(entry);
            Assert.Equal("test@example.com", entry.State.CustomerEmail);
            Assert.Equal(1, entry.State.MessageCount);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    [Fact]
    public async Task Saga_MultipleMessages_SameCorrelationId_StateAccumulates()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "order@test.com"));
            await publisher.PublishAsync(new EfSagaOrderConfirmed(orderId));

            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString()))
                    ?.State.MessageCount == 2);

            var entry = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString());

            Assert.NotNull(entry);
            Assert.Equal("order@test.com", entry.State.CustomerEmail);
            Assert.True(entry.State.IsConfirmed);
            Assert.Equal(2, entry.State.MessageCount);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    [Fact]
    public async Task Saga_DifferentCorrelationIds_MaintainSeparateState()
    {
        var orderId1 = Guid.NewGuid();
        var orderId2 = Guid.NewGuid();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId1, "first@test.com"));
            await publisher.PublishAsync(new EfSagaOrderStarted(orderId2, "second@test.com"));

            await WaitUntilAsync(async () =>
            {
                var e1 = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId1.ToString());
                var e2 = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId2.ToString());
                return e1?.State.MessageCount == 1 && e2?.State.MessageCount == 1;
            });

            var entry1 = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId1.ToString());
            var entry2 = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId2.ToString());

            Assert.NotNull(entry1);
            Assert.NotNull(entry2);
            Assert.Equal("first@test.com", entry1.State.CustomerEmail);
            Assert.Equal("second@test.com", entry2.State.CustomerEmail);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    [Fact]
    public async Task Saga_Delete_RemovesStateFromRepository()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<EfSagaOrderWithDeletionSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderWithDeletionSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "cancel@test.com"));

            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaOrderWithDeletionSaga>(provider, orderId.ToString()))
                    ?.State.MessageCount == 1);

            await publisher.PublishAsync(new EfSagaOrderCancelled(orderId));

            await WaitUntilAsync(async () =>
                await GetSagaEntryAsync<EfSagaOrderWithDeletionSaga>(provider, orderId.ToString()) == null,
                timeoutMs: 3000);

            var entry = await GetSagaEntryAsync<EfSagaOrderWithDeletionSaga>(provider, orderId.ToString());
            Assert.Null(entry);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    [Fact]
    public async Task Saga_Complete_PreservesStateAndStopsProcessingNewMessages()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<EfSagaCompletingOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaCompletingOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "complete@test.com"));
            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString()))
                    ?.State.MessageCount == 1);

            await publisher.PublishAsync(new EfSagaOrderConfirmed(orderId));
            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString()))
                    ?.IsCompleted == true);

            var completedEntry = await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString());
            Assert.NotNull(completedEntry);
            Assert.True(completedEntry.IsCompleted);
            Assert.Equal("complete@test.com", completedEntry.State.CustomerEmail);
            Assert.Equal(2, completedEntry.State.MessageCount);

            // A further message must be silently dropped (saga is completed).
            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "new@test.com"));
            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString()))
                    ?.State.CustomerEmail == "new@test.com",
                timeoutMs: 500, intervalMs: 10);

            var entryAfter = await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString());
            Assert.NotNull(entryAfter);
            Assert.True(entryAfter.IsCompleted);
            Assert.Equal("complete@test.com", entryAfter.State.CustomerEmail);
            Assert.Equal(2, entryAfter.State.MessageCount);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    [Fact]
    public async Task Saga_WithTtl_ExpiredEntryTreatedAsAbsent()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>()
                    .WithTimeToLive(TimeSpan.FromMilliseconds(100)));
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "ttl@test.com"));

            await WaitUntilAsync(async () =>
                await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString()) != null);

            // Wait for the entry to expire.
            await Task.Delay(200);

            var entry = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString());
            Assert.Null(entry);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }
}
