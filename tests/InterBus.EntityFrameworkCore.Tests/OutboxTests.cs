using System.Collections.Concurrent;
using InterBus.EntityFrameworkCore.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace InterBus.EntityFrameworkCore.Tests;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);

public sealed class OrderPlacedTracker
{
    private readonly ConcurrentBag<OrderPlaced> _messages = [];
    public IReadOnlyCollection<OrderPlaced> Messages => _messages;
    public void Track(OrderPlaced msg) => _messages.Add(msg);
}

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

public sealed class TestDbContext(DbContextOptions<TestDbContext> options)
    : DbContext(options), IOutboxDbContext
{
    public DbSet<OutboxGroup> OutboxGroups => Set<OutboxGroup>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddInterBusOutbox();
}

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

    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts, SqliteConnection Connection)>
        StartAsync(
            Action<IInterBusBuilder> configureBus,
            Action<IServiceCollection>? extraServices = null)
    {
        // Use a single kept-open SqliteConnection so all DbContext instances share the same
        // in-memory database for the duration of the test.
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseSqlite(connection));
        extraServices?.Invoke(services);
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<TestDbContext>()
                .Database.EnsureCreatedAsync();
        }

        var cts = new CancellationTokenSource();

        return (provider, cts, connection);
    }

    private static async Task StartHostedServicesAsync(ServiceProvider provider, CancellationToken cancellationToken)
    {
        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cancellationToken);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts,
        SqliteConnection connection)
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
        await connection.DisposeAsync();
    }

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
            // Long poll interval so the processor does not run during assertions.
            bus.UseOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromHours(1)));
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

            var outboxCount = await dbContext.OutboxMessages
                .CountAsync(TestContext.Current.CancellationToken);
            Assert.Equal(1, outboxCount);

            var groupCount = await dbContext.OutboxGroups
                .CountAsync(TestContext.Current.CancellationToken);
            Assert.Equal(1, groupCount);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// The outbox processor must forward stored messages to the transport and delete them.
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_ForwardsMessages_ToTransport()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.UseOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
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

            await StartHostedServicesAsync(provider, cts.Token);

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
    /// Multiple messages published in the same scope share a GroupId and are all forwarded.
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_ForwardsMultipleMessages_FromSameGroup()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.UseOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
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

                // All messages share the same GroupId (same middleware scope).
                var groupIds = dbContext.OutboxMessages.Local.Select(m => m.GroupId).Distinct().ToList();
                Assert.Single(groupIds);

                await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            await StartHostedServicesAsync(provider, cts.Token);

            await WaitUntilAsync(() => tracker.Messages.Count >= count);

            Assert.Equal(count, tracker.Messages.Count);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// After the outbox processor forwards messages, outbox message and group tables must be empty.
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_DeletesProcessedMessages()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.UseOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
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

            await StartHostedServicesAsync(provider, cts.Token);

            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            // Allow a brief moment for the delete + commit.
            await Task.Delay(200, TestContext.Current.CancellationToken);

            await using (var scope = provider.CreateAsyncScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                var remaining = await dbContext.OutboxMessages
                    .CountAsync(TestContext.Current.CancellationToken);
                Assert.Equal(0, remaining);

                var remainingGroups = await dbContext.OutboxGroups
                    .CountAsync(TestContext.Current.CancellationToken);
                Assert.Equal(0, remainingGroups);
            }
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// A message published with a delay must have that delay stored in the outbox row
    /// so the processor can pass it through to the transport.
    /// </summary>
    [Fact]
    public async Task Publish_WithDelay_StoresDelayInOutboxMessage()
    {
        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            // Long poll interval — we only want to inspect the stored row, not forward.
            bus.UseOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromHours(1)));
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            var delay = TimeSpan.FromMinutes(5);
            await publisher.Publish(
                new OrderPlaced(Guid.NewGuid(), "delay@example.com", 1m),
                ctx => ctx.Delay = delay,
                TestContext.Current.CancellationToken);

            await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);

            var row = await dbContext.OutboxMessages.SingleAsync(TestContext.Current.CancellationToken);
            Assert.Equal(delay, row.Delay);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// Empty outbox groups (with no message rows) are still claimed and removed.
    /// </summary>
    [Fact]
    public async Task OutboxProcessor_DeletesEmptyGroup()
    {
        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.UseOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
        });

        try
        {
            var groupId = Guid.NewGuid();

            await using (var scope = provider.CreateAsyncScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                dbContext.OutboxGroups.Add(new OutboxGroup { Id = groupId });
                await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            await StartHostedServicesAsync(provider, cts.Token);

            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (DateTime.UtcNow < deadline)
            {
                await using var scope = provider.CreateAsyncScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                var exists = await dbContext.OutboxGroups
                    .AnyAsync(g => g.Id == groupId, TestContext.Current.CancellationToken);
                if (!exists)
                    break;

                await Task.Delay(25, TestContext.Current.CancellationToken);
            }

            await using (var scope = provider.CreateAsyncScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                var exists = await dbContext.OutboxGroups
                    .AnyAsync(g => g.Id == groupId, TestContext.Current.CancellationToken);
                Assert.False(exists);
            }
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }
}
