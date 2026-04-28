using InterBus.EntityFrameworkCore.Extensions;
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

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_tracker);

        services.AddDbContext<TestDbContext>(o =>
            o.UseSqlServer(_container.GetConnectionString()));

        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.UseOutbox<TestDbContext>(outbox => outbox
                .WithPollInterval(TimeSpan.FromMilliseconds(100))
                .UseSqlServer());
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.Database.EnsureCreatedAsync();

            // Clear any leftover rows from previous tests so each test starts clean.
            await db.OutboxGroups.ExecuteDeleteAsync();
        }

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDown()
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

        _cts.Dispose();
        await _provider.DisposeAsync();
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
    /// Multiple messages published in the same scope share a single <see cref="OutboxGroup"/>
    /// and are all forwarded atomically by the processor.
    /// </summary>
    [Test]
    public async Task OutboxProcessor_ForwardsMultipleMessagesInGroup()
    {
        const int count = 5;

        await using (var scope = _provider.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestDbContext>();

            for (var i = 0; i < count; i++)
                await publisher.PublishAsync(
                    new OrderPlaced(Guid.NewGuid(), $"ss-user{i}@example.com", i * 10m),
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
}
