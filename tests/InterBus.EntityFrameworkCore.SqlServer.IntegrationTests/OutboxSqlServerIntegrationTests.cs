using System.Diagnostics;
using System.Text.Json;
using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Outbox;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.MsSql;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

/// <summary>
/// Integration tests for the transactional outbox using a real SQL Server database via
/// Testcontainers. Validates the <see cref="SqlServerOutboxLockProvider{TDbContext}"/> with
/// <c>UPDLOCK, READPAST</c> row-level locking.
/// </summary>
[TestFixtureSource(typeof(SqlServerVersions), nameof(SqlServerVersions.All))]
public class OutboxSqlServerIntegrationTests(string imageTag)
{
    // SQL Server container is expensive to start, so we share one instance across all tests.
    private readonly MsSqlContainer _container = new MsSqlBuilder(imageTag).Build();
    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;
    private OrderPlacedTracker _tracker = null!;
    private SendSequenceTracker _sendTracker = null!;
    private bool _servicesStarted;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        _tracker = new OrderPlacedTracker();
        _sendTracker = new SendSequenceTracker();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_tracker);
        services.AddSingleton(_sendTracker);

        services.AddDbContext<TestDbContext>(o =>
            o.UseSqlServer(_container.GetConnectionString()));

        services.AddInterBus(bus =>
        {
            var testName = TestContext.CurrentContext.Test.Name;
            var isHighConcurrencyTest = testName.Contains("Throughput", StringComparison.Ordinal)
                                        || testName.Contains("Parallel", StringComparison.Ordinal);
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.AddConsumer<SequencedOrderConsumer>();
            bus.UseSendMiddleware<HeaderDelaySendMiddleware>();
            bus.UseSendMiddleware<SequenceTraceSendMiddleware>();
            bus.UseEfCoreOutbox<TestDbContext>(outbox => outbox
                .WithShardCount(128)
                .WithMaxConcurrentWorkers(isHighConcurrencyTest ? 8 : 1)
                .WithEmptyPollDelay(isHighConcurrencyTest ? TimeSpan.FromMilliseconds(5) : TimeSpan.FromMilliseconds(100))
                .UseSqlServer());
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();
        _servicesStarted = false;

        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.Database.EnsureCreatedAsync();

            // Clear any leftover rows from previous tests so each test starts clean.
            await db.OutboxGroups.ExecuteDeleteAsync();
        }

    }

    [TearDown]
    public async Task TearDown()
    {
        if (_servicesStarted)
        {
            await _cts.CancelAsync();
            foreach (var svc in _provider.GetServices<IHostedService>())
            {
                try
                {
                    await svc.StopAsync(CancellationToken.None);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        _cts.Dispose();
        await _provider.DisposeAsync();
    }

    private async Task StartHostedServicesAsync()
    {
        if (_servicesStarted)
            return;

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);

        _servicesStarted = true;
    }

    /// <summary>
    /// Publishing a message with the outbox middleware stores both an <see cref="OutboxGroup"/>
    /// row and an <see cref="OutboxMessage"/> row in the SQL Server database. The consumer
    /// should NOT receive the message until <c>SaveChangesAsync</c> has been called and the
    /// processor has forwarded the message.
    /// </summary>
    [Test]
    public async Task Publish_StoresOutboxRowsInDatabase()
    {
        await using var scope = _provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

        var msg = new OrderPlaced(Guid.NewGuid(), "ss-store@example.com", 10m);

        await publisher.PublishAsync(msg, TestContext.CurrentContext.CancellationToken);

        // Row is tracked locally, not yet persisted.
        Assert.That(dbContext.OutboxMessages.Local, Has.Count.EqualTo(1));
        Assert.That(dbContext.OutboxGroups.Local, Has.Count.EqualTo(1));

        // Consumer must not have received anything yet.
        Assert.That(_tracker.Messages, Is.Empty);
    }

    /// <summary>
    /// After <c>SaveChangesAsync</c>, the background processor forwards the message to the
    /// transport (InMemory) and deletes both the message and group rows.
    /// </summary>
    [Test]
    public async Task OutboxProcessor_ForwardsAndDeletesMessages()
    {
        await StartHostedServicesAsync();

        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await publisher.PublishAsync(
                new OrderPlaced(Guid.NewGuid(), "ss-deliver@example.com", 50m),
                TestContext.CurrentContext.CancellationToken);

            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count == 1);

        Assert.That(_tracker.Messages, Has.Count.EqualTo(1));
        Assert.That(_tracker.Messages.Single().CustomerEmail, Is.EqualTo("ss-deliver@example.com"));

        // Allow a brief moment for the delete + commit.
        await Task.Delay(300, TestContext.CurrentContext.CancellationToken);

        await using (var scope = _provider.CreateAsyncScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            using (Assert.EnterMultipleScope())
            {
                Assert.That(await dbContext.OutboxMessages.CountAsync(TestContext.CurrentContext.CancellationToken),
                    Is.EqualTo(0));
                Assert.That(await dbContext.OutboxGroups.CountAsync(TestContext.CurrentContext.CancellationToken),
                    Is.EqualTo(0));
            }
        }
    }

    /// <summary>
    /// Multiple messages published with the same message-group-id share a single
    /// <see cref="OutboxGroup"/> and are all forwarded atomically by the processor.
    /// </summary>
    [Test]
    public async Task OutboxProcessor_ForwardsMultipleMessagesInGroup()
    {
        const int count = 5;
        await StartHostedServicesAsync();

        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            for (var i = 0; i < count; i++)
                await publisher.PublishAsync(
                    new OrderPlaced(Guid.NewGuid(), $"ss-user{i}@example.com", i * 10m),
                    ctx => ctx.Headers[InterBusHeaders.MessageGroupId] = "order-group",
                    TestContext.CurrentContext.CancellationToken);

            // All messages must share the same group.
            var groupIds = dbContext.OutboxMessages.Local.Select(m => m.GroupId).Distinct();
            Assert.That(groupIds, Has.Exactly(1).Items);

            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= count);

        Assert.That(_tracker.Messages, Has.Count.EqualTo(count));
    }

    /// <summary>
    /// Messages committed in two separate scopes (two distinct groups) are each forwarded
    /// in their own processing cycle without interference.
    /// </summary>
    [Test]
    public async Task OutboxProcessor_ProcessesMultipleGroupsSequentially()
    {
        await StartHostedServicesAsync();

        // First group
        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await publisher.PublishAsync(
                new OrderPlaced(Guid.NewGuid(), "ss-group1@example.com", 1m),
                TestContext.CurrentContext.CancellationToken);
            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        // Second group
        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await publisher.PublishAsync(
                new OrderPlaced(Guid.NewGuid(), "ss-group2@example.com", 2m),
                TestContext.CurrentContext.CancellationToken);
            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count >= 2);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(_tracker.Messages, Has.Count.EqualTo(2));
            Assert.That(_tracker.Messages.Select(m => m.CustomerEmail),
                Is.EquivalentTo(["ss-group1@example.com", "ss-group2@example.com"]));
        }
    }

    [Test]
    [Category("Performance")]
    public async Task OutboxProcessor_Throughput_Baseline()
    {
        const int messageCount = 20_000;
        const int groupCount = 128;
        const double minMessagesPerSecond = 100;
        await StartHostedServicesAsync();

        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            for (var i = 0; i < messageCount; i++)
            {
                var groupId = $"ss-perf-group-{i % groupCount}";
                await publisher.PublishAsync(
                    new OrderPlaced(Guid.NewGuid(), $"ss-perf-{i}@example.com", i),
                    ctx => ctx.Headers[InterBusHeaders.MessageGroupId] = groupId,
                    TestContext.CurrentContext.CancellationToken);
            }

            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        var stopwatch = Stopwatch.StartNew();
        await TestHelpers.WaitUntilAsync(() => _tracker.Messages.Count == messageCount, timeoutMs: 30_000, intervalMs: 10);
        stopwatch.Stop();

        Assert.That(_tracker.Messages, Has.Count.EqualTo(messageCount));

        var throughput = messageCount / Math.Max(0.001, stopwatch.Elapsed.TotalSeconds);
        await TestContext.Progress.WriteLineAsync(
            $"[SQL Server throughput] messages={messageCount}, groups={groupCount}, elapsed={stopwatch.Elapsed.TotalSeconds:F3}s, throughput={throughput:F2} msg/s");
        Assert.That(
            throughput,
            Is.GreaterThanOrEqualTo(minMessagesPerSecond),
            $"Throughput below baseline. Measured {throughput:F2} msg/s, expected >= {minMessagesPerSecond:F2} msg/s");
    }

    [Test]
    public async Task OutboxProcessor_PreservesPerGroupFifo_UnderContention()
    {
        const string hotGroupId = "hot-group";
        const int hotCount = 24;
        const int noiseCount = 48;
        const int noiseGroups = 12;

        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            for (var i = 0; i < noiseCount; i++)
            {
                var noiseGroup = $"noise-group-{i % noiseGroups}";
                await publisher.PublishAsync(
                    new SequencedOrder(noiseGroup, i),
                    ctx =>
                    {
                        ctx.Headers[InterBusHeaders.MessageGroupId] = noiseGroup;
                        ctx.Headers[SequenceTraceSendMiddleware.SequenceHeaderName] = i.ToString();
                    },
                    TestContext.CurrentContext.CancellationToken);
            }

            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        for (var i = 0; i < hotCount; i++)
        {
            await using var scope = _provider.CreateAsyncScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            await publisher.PublishAsync(
                new SequencedOrder(hotGroupId, i),
                ctx =>
                {
                    ctx.Headers[InterBusHeaders.MessageGroupId] = hotGroupId;
                    ctx.Headers[SequenceTraceSendMiddleware.SequenceHeaderName] = i.ToString();
                },
                TestContext.CurrentContext.CancellationToken);

            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        var expectedTotal = hotCount + noiseCount;
        int[] expectedHotOrder;
        await using (var scope = _provider.CreateAsyncScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var orderedMessages = await dbContext.OutboxMessages
                .Include(m => m.Group)
                .OrderBy(m => m.Group!.Id)
                .ToListAsync(TestContext.CurrentContext.CancellationToken);

            expectedHotOrder = orderedMessages
                .Select(m => JsonSerializer.Deserialize<Dictionary<string, string>>(m.HeadersJson)!)
                .Where(h => h.TryGetValue(InterBusHeaders.MessageGroupId, out var groupId) && groupId == hotGroupId)
                .Select(h => int.Parse(h[SequenceTraceSendMiddleware.SequenceHeaderName]))
                .ToArray();
        }

        await StartHostedServicesAsync();
        await TestHelpers.WaitUntilAsync(() => _sendTracker.Count == expectedTotal, timeoutMs: 30_000, intervalMs: 20);

        var hotObserved = _sendTracker.Observations
            .Where(o => o.MessageGroupId == hotGroupId)
            .Select(o => o.Sequence)
            .ToArray();

        Assert.That(hotObserved, Is.EqualTo(expectedHotOrder),
            "Messages within the same message-group-id must be consumed in FIFO order");
    }

    [Test]
    public async Task OutboxProcessor_ProcessesDifferentGroupsInParallel()
    {
        const int groupCount = 16;
        const int delayMs = 150;
        var sequentialMs = groupCount * delayMs;

        await StartHostedServicesAsync();
        var stopwatch = Stopwatch.StartNew();

        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            for (var i = 0; i < groupCount; i++)
            {
                var groupId = $"parallel-group-{i}";
                await publisher.PublishAsync(
                    new SequencedOrder(groupId, i),
                    ctx =>
                    {
                        ctx.Headers[InterBusHeaders.MessageGroupId] = groupId;
                        ctx.Headers[HeaderDelaySendMiddleware.DelayHeaderName] = delayMs.ToString();
                        ctx.Headers[SequenceTraceSendMiddleware.SequenceHeaderName] = i.ToString();
                    },
                    TestContext.CurrentContext.CancellationToken);
            }

            await dbContext.SaveChangesAsync(TestContext.CurrentContext.CancellationToken);
        }

        await TestHelpers.WaitUntilAsync(() => _sendTracker.Count == groupCount, timeoutMs: 30_000, intervalMs: 10);
        stopwatch.Stop();

        await TestContext.Progress.WriteLineAsync(
            $"[SQL Server parallelism] groups={groupCount}, delayMs={delayMs}, elapsedMs={stopwatch.ElapsedMilliseconds}, sequentialMs={sequentialMs}");

        Assert.That(
            stopwatch.ElapsedMilliseconds,
            Is.LessThan(sequentialMs - 50),
            "Different message groups should be processed in parallel instead of fully sequentially");
    }
}
