using System.Collections.Concurrent;
using InterBus.Consuming;
using InterBus.DeadLettering;
using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Outbox;
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

public record OrderShipped(Guid OrderId);

public sealed class OrderPlacedTracker
{
    private readonly ConcurrentBag<OrderPlaced> _messages = [];
    public IReadOnlyCollection<OrderPlaced> Messages => _messages;
    public void Track(OrderPlaced msg) => _messages.Add(msg);
}

public sealed class OrderShippedTracker
{
    private readonly ConcurrentBag<OrderShipped> _messages = [];
    public IReadOnlyCollection<OrderShipped> Messages => _messages;
    public void Track(OrderShipped msg) => _messages.Add(msg);
}

public sealed class OrderPlacedConsumer(OrderPlacedTracker tracker) : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Consumer that publishes <see cref="OrderShipped"/> to the outbox when it receives
/// <see cref="OrderPlaced"/>. Saves the DbContext explicitly so the outbox row is committed.
/// </summary>
public sealed class OrderPlacedPublishingConsumer(TestDbContext dbContext, IPublisher publisher)
    : IConsumer<OrderPlaced>
{
    public async Task Consume(OrderPlaced message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        await publisher.PublishAsync(new OrderShipped(message.OrderId), cancellationToken);
        await dbContext.SaveChangesAsync(cancellationToken);
    }
}

public sealed class OrderShippedConsumer(OrderShippedTracker tracker) : IConsumer<OrderShipped>
{
    public Task Consume(OrderShipped message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
}

public sealed class TestDbContext(DbContextOptions<TestDbContext> options)
    : DbContext(options)
{
    public DbSet<OutboxGroup> OutboxGroups => Set<OutboxGroup>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddInterBusOutbox();
}

/// <summary>
/// A consumer that always throws, used to verify dead-letter delivery when the outbox is configured.
/// Uses a distinct name to avoid conflicts with other test projects.
/// </summary>
public sealed class FaultingOrderPlacedConsumerWithOutbox : IConsumer<OrderPlaced>
{
    public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default) =>
        throw new InvalidOperationException("Intentional failure for dead-letter outbox test");
}

/// <summary>
/// Tracker for <see cref="DeadLetter{T}"/> messages in the outbox test project.
/// Uses a distinct type to avoid singleton conflicts with other test projects.
/// </summary>
public sealed class DeadLetterOrderPlacedTrackerForOutbox
{
    private readonly ConcurrentBag<DeadLetter<OrderPlaced>> _messages = [];
    public IReadOnlyCollection<DeadLetter<OrderPlaced>> Messages => _messages;
    public void Track(DeadLetter<OrderPlaced> msg) => _messages.Add(msg);
}

/// <summary>Consumer of <see cref="DeadLetter{OrderPlaced}"/> used by the outbox dead-letter test.</summary>
public sealed class DeadLetterOrderPlacedConsumerForOutbox(DeadLetterOrderPlacedTrackerForOutbox tracker)
    : IConsumer<DeadLetter<OrderPlaced>>
{
    public Task Consume(DeadLetter<OrderPlaced> message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        tracker.Track(message);
        return Task.CompletedTask;
    }
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
            Action<InterBusBuilder> configureBus,
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
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromHours(1)));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 42m);

            // Publish adds the outbox row but does NOT save changes yet.
            await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);

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
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                var msg = new OrderPlaced(Guid.NewGuid(), "deliver@example.com", 99m);
                await publisher.PublishAsync(msg, TestContext.Current.CancellationToken);
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
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
        }, services => services.AddSingleton(tracker));

        try
        {
            const int count = 5;

            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                for (var i = 0; i < count; i++)
                    await publisher.PublishAsync(
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
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
        }, services => services.AddSingleton(tracker));

        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                await publisher.PublishAsync(
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
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromHours(1)));
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            var delay = TimeSpan.FromMinutes(5);
            await publisher.PublishAsync(
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
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromMilliseconds(50)));
        });

        try
        {
            var groupId = Guid.NewGuid();

            await using (var scope = provider.CreateAsyncScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                dbContext.OutboxGroups.Add(new OutboxGroup { Id = groupId, VisibleAt = DateTime.UtcNow });
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

    /// <summary>
    /// When a consumer opts into the outbox via <c>UseOutboxWithInlineDelivery&lt;TDbContext&gt;()</c>
    /// and publishes a message during its consume handler, the message is first written to the outbox
    /// atomically with the consumer's own database changes. After the consumer's <c>Consume</c>
    /// handler returns, <c>OutboxConsumeMiddleware{TDbContext}</c> attempts inline
    /// delivery: if delivery succeeds the group is deleted from the DB, so the outbox is empty
    /// after the execute pipeline completes. No outbox processor involvement is needed.
    /// </summary>
    [Fact]
    public async Task ConsumerPublish_InlineDelivery_SucceedsWithoutOutboxEntry()
    {
        var shippedTracker = new OrderShippedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedPublishingConsumer>(c => c.UseEfCoreOutboxWithInlineDelivery<TestDbContext>());
            bus.AddConsumer<OrderShippedConsumer>();
            // Long poll interval so the outbox processor does not run during assertions.
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromHours(1)));
        }, services => services.AddSingleton(shippedTracker));

        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
                var descriptor = scope.ServiceProvider.GetRequiredService<InterBusOptions>()
                    .Consumers.Single(d => d.ConsumerType == typeof(OrderPlacedPublishingConsumer));

                var orderId = Guid.NewGuid();
                var headers = new MessageHeaders(new Dictionary<string, string>
                {
                    [InterBusHeaders.MessageType] = "OrderPlaced",
                    [InterBusHeaders.MessageId] = Guid.NewGuid().ToString(),
                });
                var context = new ConsumeContext<OrderPlaced>(
                    descriptor, new OrderPlaced(orderId, "consumer@example.com", 10m), headers);

                await executor.ExecuteAsync(context, (ctx, ct) =>
                {
                    var consumer = scope.ServiceProvider.GetRequiredService<OrderPlacedPublishingConsumer>();
                    return consumer.Consume(ctx.Message, ctx, ct);
                }, TestContext.Current.CancellationToken);

                // Outbox entry was written then immediately delivered and deleted — DB should be empty.
                var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                var outboxCount = await dbContext.OutboxMessages.CountAsync(TestContext.Current.CancellationToken);
                Assert.Equal(0, outboxCount);

                var groupCount = await dbContext.OutboxGroups.CountAsync(TestContext.Current.CancellationToken);
                Assert.Equal(0, groupCount);
            }

            // OrderShippedConsumer should NOT have received anything yet (pump not started).
            Assert.Empty(shippedTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// Consumer-published messages that are delivered inline are queued in the InMemory transport.
    /// With topology and pump running, the downstream consumer receives the message.
    /// The outbox processor is disabled for this scenario because inline delivery already
    /// forwarded the message.
    /// </summary>
    [Fact]
    public async Task ConsumerPublish_InlineDelivery_MessageDeliveredByPump()
    {
        var shippedTracker = new OrderShippedTracker();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedPublishingConsumer>(c => c.UseEfCoreOutboxWithInlineDelivery<TestDbContext>());
            bus.AddConsumer<OrderShippedConsumer>();
            bus.UseEfCoreOutbox<TestDbContext>(o => o.DisableOutboxProcessor());
        }, services => services.AddSingleton(shippedTracker));

        try
        {
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
                var descriptor = scope.ServiceProvider.GetRequiredService<InterBusOptions>()
                    .Consumers.Single(d => d.ConsumerType == typeof(OrderPlacedPublishingConsumer));

                var orderId = Guid.NewGuid();
                var headers = new MessageHeaders(new Dictionary<string, string>
                {
                    [InterBusHeaders.MessageType] = "OrderPlaced",
                    [InterBusHeaders.MessageId] = Guid.NewGuid().ToString(),
                });
                var context = new ConsumeContext<OrderPlaced>(
                    descriptor, new OrderPlaced(orderId, "consumer@example.com", 10m), headers);

                await executor.ExecuteAsync(context, (ctx, ct) =>
                {
                    var consumer = scope.ServiceProvider.GetRequiredService<OrderPlacedPublishingConsumer>();
                    return consumer.Consume(ctx.Message, ctx, ct);
                }, TestContext.Current.CancellationToken);
            }

            await WaitUntilAsync(() => shippedTracker.Messages.Count == 1);

            Assert.Single(shippedTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// When inline delivery fails (e.g. no transport registered), the outbox entry written
    /// atomically during <c>Consume</c> is left in the database with <c>VisibleAt</c> set to a
    /// future timestamp so the processor does not race with the remaining inline-delivery attempt.
    /// </summary>
    [Fact]
    public async Task ConsumerPublish_InlineDeliveryFails_FallsBackToOutboxWithVisibleAt()
    {
        // No transport registered — inline delivery will throw, triggering the outbox fallback.
        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddConsumer<OrderPlacedPublishingConsumer>(c => c.UseEfCoreOutboxWithInlineDelivery<TestDbContext>());
            // Use the default ConsumerVisibilityTimeout (5 s); just suppress the background processor.
            bus.UseEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromHours(1)));
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
            var descriptor = scope.ServiceProvider.GetRequiredService<InterBusOptions>()
                .Consumers.Single(d => d.ConsumerType == typeof(OrderPlacedPublishingConsumer));

            var orderId = Guid.NewGuid();
            var headers = new MessageHeaders(new Dictionary<string, string>
            {
                [InterBusHeaders.MessageType] = "OrderPlaced",
                [InterBusHeaders.MessageId] = Guid.NewGuid().ToString(),
            });
            var context = new ConsumeContext<OrderPlaced>(
                descriptor, new OrderPlaced(orderId, "fallback@example.com", 10m), headers);

            await executor.ExecuteAsync(context, (ctx, ct) =>
            {
                var consumer = scope.ServiceProvider.GetRequiredService<OrderPlacedPublishingConsumer>();
                return consumer.Consume(ctx.Message, ctx, ct);
            }, TestContext.Current.CancellationToken);

            // Inline delivery failed — outbox entry should remain with VisibleAt in the future.
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var group = await dbContext.OutboxGroups.SingleAsync(TestContext.Current.CancellationToken);
            Assert.True(group.VisibleAt > DateTime.UtcNow);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// Non-consumer publish calls (via <see cref="IPublisher"/>) have
    /// <c>VisibleAt</c> set to <c>UtcNow</c> (immediately eligible), even when
    /// <see cref="OutboxOptions.ConsumerVisibilityTimeout"/> is configured.
    /// </summary>
    [Fact]
    public async Task BusPublish_GroupHasImmediateVisibleAt()
    {
        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.UseEfCoreOutbox<TestDbContext>(o => o
                .WithPollInterval(TimeSpan.FromHours(1))
                .WithConsumerVisibilityTimeout(TimeSpan.FromSeconds(5)));
        });

        try
        {
            await using var scope = provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await publisher.PublishAsync(
                new OrderPlaced(Guid.NewGuid(), "bus@example.com", 1m),
                TestContext.Current.CancellationToken);
            await dbContext.SaveChangesAsync(TestContext.Current.CancellationToken);

            var group = await dbContext.OutboxGroups.SingleAsync(TestContext.Current.CancellationToken);
            // Bus-context publishes are immediately eligible; VisibleAt should be <= UtcNow.
            Assert.True(group.VisibleAt <= DateTime.UtcNow);
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }

    /// <summary>
    /// When a consumer with the outbox configured throws an unhandled exception,
    /// <see cref="DeadLetterConsumeMiddleware"/> publishes a <see cref="DeadLetter{T}"/>
    /// via <see cref="IPublisher"/>. The message must be forwarded directly to the transport
    /// as the outbox cannot be assumed to be used and controlled correctly
    /// </summary>
    [Fact]
    public async Task DeadLetterPublish_WithOutboxConfigured_BypassesOutboxAndDeliveredToConsumer()
    {
        var deadLetterTracker = new DeadLetterOrderPlacedTrackerForOutbox();

        var (provider, cts, conn) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            // Faulting consumer has outbox configured — the dead-letter publish must still
            // reach the transport even without SaveChangesAsync being called.
            bus.AddConsumer<FaultingOrderPlacedConsumerWithOutbox>(c => c.UseEfCoreOutbox<TestDbContext>());
            bus.AddConsumer<DeadLetterOrderPlacedConsumerForOutbox>();
            // AddOutbox (not UseOutbox) — only the consumer-level outbox is under test here.
            // Long poll interval effectively disables the background processor so it does not
            // interfere with the assertions.
            bus.AddEfCoreOutbox<TestDbContext>(o => o.WithPollInterval(TimeSpan.FromHours(1)));
        }, services => services.AddSingleton(deadLetterTracker));

        try
        {
            await StartHostedServicesAsync(provider, cts.Token);

            await using (var scope = provider.CreateAsyncScope())
            {
                var executor = scope.ServiceProvider.GetRequiredService<ConsumePipelineExecutor>();
                var descriptor = scope.ServiceProvider.GetRequiredService<InterBusOptions>()
                    .Consumers.Single(d => d.ConsumerType == typeof(FaultingOrderPlacedConsumerWithOutbox));

                var headers = new MessageHeaders(new Dictionary<string, string>
                {
                    [InterBusHeaders.MessageType] = "OrderPlaced",
                    [InterBusHeaders.MessageId] = Guid.NewGuid().ToString(),
                });
                var context = new ConsumeContext<OrderPlaced>(
                    descriptor, new OrderPlaced(Guid.NewGuid(), "faulting@example.com", 42m), headers);

                // The consumer throws; DeadLetterConsumeMiddleware catches it and publishes
                // DeadLetter<OrderPlaced>. OutboxPublishMiddleware must forward it directly
                // to the transport instead of buffering it in the outbox.
                try
                {
                    await executor.ExecuteAsync(context, (ctx, ct) =>
                    {
                        var consumer = scope.ServiceProvider
                            .GetRequiredService<FaultingOrderPlacedConsumerWithOutbox>();
                        return consumer.Consume(ctx.Message, ctx, ct);
                    }, TestContext.Current.CancellationToken);
                }
                catch (DeadLetterException)
                {
                    /* expected — the consumer threw */
                }
            }

            await WaitUntilAsync(() => deadLetterTracker.Messages.Count >= 1);

            Assert.NotEmpty(deadLetterTracker.Messages);
            var dl = deadLetterTracker.Messages.First();
            Assert.Equal("faulting@example.com", dl.OriginalMessage.CustomerEmail);
            Assert.Equal("Unhandled exception in consume pipeline", dl.Reason);

            // No outbox entry should have been created for the dead-letter message —
            // OutboxPublishMiddleware forwarded it directly to the transport.
            await using var assertScope = provider.CreateAsyncScope();
            var db = assertScope.ServiceProvider.GetRequiredService<TestDbContext>();
            Assert.Equal(0, await db.OutboxMessages.CountAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            await StopAsync(provider, cts, conn);
        }
    }
}
