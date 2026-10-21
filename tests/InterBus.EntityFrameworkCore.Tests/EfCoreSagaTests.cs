using InterBus.Generated;
using InterBus.EntityFrameworkCore.Extensions;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.EntityFrameworkCore.Tests;

/// <summary>
/// Integration tests for the EF Core saga repository using SQLite in-memory.
/// Verifies state persistence, multi-message accumulation, deletion, completion, and TTL
/// with the default in-process lock provider.
/// </summary>
public class EfCoreSagaTests
{
    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts, SqliteConnection KeepAlive)>
        StartAsync(Action<InterBusBuilder> configureBus)
    {
        // Use a unique named in-memory database with shared cache so that each DbContext
        // instance (from the message pump and from test-polling helpers) opens its own
        // SqliteConnection. A single shared SqliteConnection is not thread-safe; when the
        // message-pump background thread and the test-polling thread access it concurrently
        // the connection can enter an invalid state, causing GetAsync to return null and the
        // completed saga to incorrectly process the dropped message.
        var dbName = $"interbus_saga_test_{Guid.NewGuid():N}";
        var connectionString = $"Data Source={dbName};Mode=Memory;Cache=Shared";

        // Keep one connection open for the lifetime of the test so the shared in-memory
        // database is not destroyed while the service provider is still running.
        var keepAlive = new SqliteConnection(connectionString);
        await keepAlive.OpenAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<SagaTestDbContext>(o => o.UseSqlite(connectionString));
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

        return (provider, cts, keepAlive);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts,
        SqliteConnection keepAlive)
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
        await keepAlive.DisposeAsync();
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
            bus.AddEfCore<SagaTestDbContext>();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "test@example.com"),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString()))
                ?.State.MessageCount == 1, cancellationToken: TestContext.Current.CancellationToken);

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
            bus.AddEfCore<SagaTestDbContext>();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "order@test.com"),
                TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new EfSagaOrderConfirmed(orderId), TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString()))
                ?.State.MessageCount == 2, cancellationToken: TestContext.Current.CancellationToken);

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
            bus.AddEfCore<SagaTestDbContext>();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId1, "first@test.com"),
                TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new EfSagaOrderStarted(orderId2, "second@test.com"),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () =>
            {
                var e1 = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId1.ToString());
                var e2 = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId2.ToString());
                return e1?.State.MessageCount == 1 && e2?.State.MessageCount == 1;
            }, cancellationToken: TestContext.Current.CancellationToken);

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
            bus.AddEfCore<SagaTestDbContext>();
            bus.AddSaga<EfSagaOrderWithDeletionSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderWithDeletionSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "cancel@test.com"),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaOrderWithDeletionSaga>(provider, orderId.ToString()))
                ?.State.MessageCount == 1, cancellationToken: TestContext.Current.CancellationToken);

            await publisher.PublishAsync(new EfSagaOrderCancelled(orderId), TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () =>
                    await GetSagaEntryAsync<EfSagaOrderWithDeletionSaga>(provider, orderId.ToString()) == null, timeoutMs: 3000, cancellationToken: TestContext.Current.CancellationToken);

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
            bus.AddEfCore<SagaTestDbContext>();
            bus.AddSaga<EfSagaCompletingOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaCompletingOrderSaga, EfSagaOrderState, SagaTestDbContext>());
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "complete@test.com"),
                TestContext.Current.CancellationToken);
            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString()))
                ?.State.MessageCount == 1, cancellationToken: TestContext.Current.CancellationToken);

            await publisher.PublishAsync(new EfSagaOrderConfirmed(orderId), TestContext.Current.CancellationToken);
            await WaitUntilAsync(async () =>
                (await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString()))
                ?.IsCompleted == true, cancellationToken: TestContext.Current.CancellationToken);

            var completedEntry = await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString());
            Assert.NotNull(completedEntry);
            Assert.True(completedEntry.IsCompleted);
            Assert.Equal("complete@test.com", completedEntry.State.CustomerEmail);
            Assert.Equal(2, completedEntry.State.MessageCount);

            // A further message must be silently dropped (saga is completed).
            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "new@test.com"),
                TestContext.Current.CancellationToken);
            await WaitUntilAsync(async () =>
                    (await GetSagaEntryAsync<EfSagaCompletingOrderSaga>(provider, orderId.ToString()))
                    ?.State.CustomerEmail == "new@test.com", timeoutMs: 500, intervalMs: 10, cancellationToken: TestContext.Current.CancellationToken);

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
            bus.AddEfCore<SagaTestDbContext>();
            bus.AddSaga<EfSagaOrderSaga, EfSagaOrderState>(saga =>
                saga.UseEfCoreRepository<EfSagaOrderSaga, EfSagaOrderState, SagaTestDbContext>()
                    .WithTimeToLive(TimeSpan.FromMilliseconds(100)));
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new EfSagaOrderStarted(orderId, "ttl@test.com"),
                TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () =>
                await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString()) != null, cancellationToken: TestContext.Current.CancellationToken);

            // Wait for the entry to expire.
            await Task.Delay(200, TestContext.Current.CancellationToken);

            var entry = await GetSagaEntryAsync<EfSagaOrderSaga>(provider, orderId.ToString());
            Assert.Null(entry);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }
}
