using System.Collections.Concurrent;
using InterBus.EFCore;
using InterBus.EFCore.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.EFCore.Tests;

// ── Message types ────────────────────────────────────────────────────────────

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);

// ── Tracking helpers ─────────────────────────────────────────────────────────

public sealed class OrderPlacedTracker
{
    private readonly ConcurrentBag<OrderPlaced> _messages = [];
    public IReadOnlyCollection<OrderPlaced> Messages => _messages;
    public void Track(OrderPlaced msg) => _messages.Add(msg);
}

// ── Consumer ─────────────────────────────────────────────────────────────────

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

// ── Test DbContext ────────────────────────────────────────────────────────────

public sealed class TestDbContext(DbContextOptions<TestDbContext> options)
    : DbContext(options), IOutboxDbContext
{
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddInterBusOutbox();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// <summary>
/// Integration tests for the transactional outbox middleware and background processor.
/// Uses SQLite in-memory for the DbContext and the InMemory transport for message delivery.
/// </summary>
public class OutboxTests
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

    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts, SqliteConnection Connection)> StartAsync(
        Action<IInterBusBuilder> configureBus,
        Action<IServiceCollection>? extraServices = null)
    {
        // Use a single, kept-open SqliteConnection so all DbContext instances share the
        // same in-memory database for the duration of the test.
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDbContext<TestDbContext>(o => o.UseSqlite(connection));

        extraServices?.Invoke(services);

        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();

        // Ensure the schema exists before starting hosted services.
        await using (var scope = provider.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<TestDbContext>()
                .Database.EnsureCreatedAsync();
        }

        var cts = new CancellationTokenSource();
        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts, connection);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts, SqliteConnection connection)
    {
        cts.Cancel();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
        await connection.DisposeAsync();
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /// <summary>
    /// When a message is published with the outbox middleware, it must be stored in the
    /// outbox table before <c>SaveChangesAsync</c> is called and must NOT be forwarded
    /// to the transport until the processor runs.
    /// </summary>
    [Fact]
    public async Task Publish_WithOutbox_StoresMessageInOutboxBeforeSave()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            // Disable automatic polling so the processor doesn't run during the assertion.
            bus.UseOutbox<TestDbContext>(o => o.PollInterval = TimeSpan.FromHours(1));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 42m);

            // Publish adds the outbox row but does NOT save changes yet.
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            // Row is tracked but not yet persisted.
            Assert.Single(dbContext.OutboxMessages.Local);

            // Save — row is now in the DB.
            await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);

            // Consumer should NOT have received the message yet (processor hasn't run).
            Assert.Empty(tracker.Messages);

            // The outbox row exists in the DB.
            var outboxCount = await dbContext.OutboxMessages
                .CountAsync(TestContext.Current.CancellationToken);
            Assert.Equal(1, outboxCount);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// The outbox processor must forward stored messages to the transport and then delete
    /// (or mark) them so they are not forwarded twice.
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_ForwardsMessages_ToTransport()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            // Short poll interval so the processor runs quickly in the test.
            bus.UseOutbox<TestDbContext>(o => o.PollInterval = TimeSpan.FromMilliseconds(50));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                var msg = new OrderPlaced(Guid.NewGuid(), "deliver@example.com", 99m);
                await publisher.Publish(msg, TestContext.Current.CancellationToken);
                await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            // Wait for the background processor to forward the message.
            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            Assert.Single(tracker.Messages);
            Assert.Equal("deliver@example.com", tracker.Messages.Single().CustomerEmail);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// Multiple messages published in the same scope and saved in the same
    /// <c>SaveChangesAsync</c> call must all be forwarded.
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_ForwardsMultipleMessages_FromSameTransaction()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.UseOutbox<TestDbContext>(o => o.PollInterval = TimeSpan.FromMilliseconds(50));
        }, services => services.AddSingleton(tracker));

        try
        {
            const int count = 5;

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                for (var i = 0; i < count; i++)
                    await publisher.Publish(
                        new OrderPlaced(Guid.NewGuid(), $"user{i}@example.com", i * 10m),
                        TestContext.Current.CancellationToken);

                // All messages are saved atomically in one call.
                await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            await WaitUntilAsync(() => tracker.Messages.Count >= count);

            Assert.Equal(count, tracker.Messages.Count);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// After the outbox processor forwards messages, the outbox table must be empty
    /// (default behaviour is to delete processed rows).
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_DeletesProcessedMessages_ByDefault()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.UseOutbox<TestDbContext>(o => o.PollInterval = TimeSpan.FromMilliseconds(50));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                await publisher.Publish(
                    new OrderPlaced(Guid.NewGuid(), "clean@example.com", 1m),
                    TestContext.Current.CancellationToken);
                await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            // Wait until the message is delivered.
            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            // Allow a brief moment for the delete to be committed.
            await Task.Delay(200, TestContext.Current.CancellationToken);

            await using (var scope = provider.CreateAsyncScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                var remaining = await dbContext.OutboxMessages
                    .CountAsync(TestContext.Current.CancellationToken);
                Assert.Equal(0, remaining);
            }
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// When <see cref="OutboxOptions.KeepProcessedMessages"/> is <c>true</c>, processed
    /// rows must be retained with <see cref="OutboxMessage.ProcessedAt"/> populated.
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_KeepsProcessedMessages_WhenOptionEnabled()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.UseOutbox<TestDbContext>(o =>
            {
                o.PollInterval = TimeSpan.FromMilliseconds(50);
                o.KeepProcessedMessages = true;
            });
        }, services => services.AddSingleton(tracker));

        try
        {
            Guid msgId;
            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                var msg = new OrderPlaced(Guid.NewGuid(), "keep@example.com", 7m);
                await publisher.Publish(msg, TestContext.Current.CancellationToken);
                await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);

                // Capture the ID so we can look it up later.
                msgId = dbContext.OutboxMessages.Local.Single().Id;
            }

            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            // Allow a brief moment for the update to be committed.
            await Task.Delay(200, TestContext.Current.CancellationToken);

            await using (var scope = provider.CreateAsyncScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                var row = await dbContext.OutboxMessages.FindAsync([msgId], TestContext.Current.CancellationToken);
                Assert.NotNull(row);
                Assert.NotNull(row!.ProcessedAt);
            }
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }
}
