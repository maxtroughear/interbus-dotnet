using InterBus.EntityFrameworkCore.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.MsSql;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

/// <summary>
/// Verifies that <see cref="SqlServerOutboxLockProvider{TDbContext}"/> correctly implements
/// <c>UPDLOCK, READPAST</c> semantics on shard locks: a shard that is already locked by
/// one database connection is skipped (not double-claimed) by a concurrent connection.
/// </summary>
[TestFixtureSource(typeof(SqlServerVersions), nameof(SqlServerVersions.All))]
public class OutboxSqlServerLockingTests(string imageTag)
{
    private static readonly Guid GroupAId = Guid.Parse("11111111-1111-7000-8000-100000000001");
    private static readonly Guid GroupBId = Guid.Parse("22222222-2222-7000-8000-200000000002");

    // Separate container so these locking tests are fully isolated from
    // OutboxSqlServerIntegrationTests (which runs a background processor).
    private readonly MsSqlContainer _container = new MsSqlBuilder(imageTag).Build();

    private ServiceProvider _provider = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp() => await _container.StartAsync();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseSqlServer(_container.GetConnectionString()));

        _provider = services.BuildServiceProvider();

        await using var scope = _provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<TestDbContext>()
            .Database.EnsureCreatedAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        try
        {
            await using var scope = _provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.OutboxGroups
                .Where(g => g.Id == GroupAId || g.Id == GroupBId)
                .ExecuteDeleteAsync();
            await db.OutboxLocks.ExecuteDeleteAsync();
        }
        finally
        {
            await _provider.DisposeAsync();
        }
    }

    /// <summary>
    /// When the only shard (with a pending group) is already locked by connection A,
    /// connection B must return <c>null</c> (no unlocked shards with work available).
    /// </summary>
    [Test]
    public async Task LockProvider_ReturnsNull_WhenOnlyShardIsLocked()
    {
        // Arrange – one shard lock row and one group in that shard.
        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            db.OutboxLocks.Add(new OutboxLock { Shard = 0 });
            db.OutboxGroups.Add(new OutboxGroup { Id = GroupAId, Shard = 0 });
            await db.SaveChangesAsync();
        }

        var lockProvider = new SqlServerOutboxLockProvider<TestDbContext>();

        // Connection A: claim shard 0 and hold the lock.
        await using var scopeA = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txA = await dbA.Database.BeginTransactionAsync();

        var claimedA = await lockProvider.TryClaimAndFetchGroupAsync(dbA, CancellationToken.None);
        Assert.That(claimedA, Is.Not.Null, "Connection A should claim the only available shard");
        Assert.That(claimedA!.Value.Group?.Id, Is.EqualTo(GroupAId));

        // Connection B: while A holds the lock on shard 0, no shards are available.
        await using var scopeB = _provider.CreateAsyncScope();
        var dbB = scopeB.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txB = await dbB.Database.BeginTransactionAsync();

        var result = await lockProvider.TryClaimAndFetchGroupAsync(dbB, CancellationToken.None);

        Assert.That(result, Is.Not.Null);

        Assert.That(result.Value.Group, Is.Null,
            "Connection B must return null because the only shard is locked by connection A (UPDLOCK, READPAST)");

        await txB.RollbackAsync();
        await txA.RollbackAsync();
    }

    /// <summary>
    /// When two shards each have a group and shard 0 is locked by connection A,
    /// connection B must skip shard 0 and successfully claim shard 1.
    /// </summary>
    [Test]
    public async Task LockProvider_SkipsLockedShard_WhenAnotherShardIsAvailable()
    {
        // Arrange – two shard lock rows; shard 0 gets groupA, shard 1 gets groupB.
        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            db.OutboxLocks.AddRange(new OutboxLock { Shard = 0 }, new OutboxLock { Shard = 1 });
            db.OutboxGroups.AddRange(
                new OutboxGroup { Id = GroupAId, Shard = 0 },
                new OutboxGroup { Id = GroupBId, Shard = 1 });
            await db.SaveChangesAsync();
        }

        var lockProvider = new SqlServerOutboxLockProvider<TestDbContext>();

        // Connection A: claim shard 0 (lowest numbered shard with work).
        await using var scopeA = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txA = await dbA.Database.BeginTransactionAsync();

        var claimedA = await lockProvider.TryClaimAndFetchGroupAsync(dbA, CancellationToken.None);
        Assert.That(claimedA, Is.Not.Null, "Connection A should claim shard 0 first");
        Assert.That(claimedA!.Value.Shard, Is.EqualTo(0), "Shard 0 (lower number) should be claimed by connection A");
        Assert.That(claimedA.Value.Group?.Id, Is.EqualTo(GroupAId));

        // Connection B: shard 0 is locked, so it should skip to shard 1.
        await using var scopeB = _provider.CreateAsyncScope();
        var dbB = scopeB.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txB = await dbB.Database.BeginTransactionAsync();

        var claimedB = await lockProvider.TryClaimAndFetchGroupAsync(dbB, CancellationToken.None, 1);

        Assert.That(claimedB, Is.Not.Null,
            "Connection B should find shard 1 even though shard 0 is locked");
        Assert.That(claimedB!.Value.Shard, Is.EqualTo(1),
            "Connection B must skip the locked shard 0 and claim shard 1 (UPDLOCK, READPAST)");
        Assert.That(claimedB.Value.Group?.Id, Is.EqualTo(GroupBId));

        await txB.RollbackAsync();
        await txA.RollbackAsync();
    }

    [Test]
    public async Task LockProvider_ReclaimsShard_AfterRollback()
    {
        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            db.OutboxLocks.Add(new OutboxLock { Shard = 0 });
            db.OutboxGroups.Add(new OutboxGroup { Id = GroupAId, Shard = 0 });
            await db.SaveChangesAsync();
        }

        var lockProvider = new SqlServerOutboxLockProvider<TestDbContext>();

        await using var scopeA = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txA = await dbA.Database.BeginTransactionAsync();

        var claimedA = await lockProvider.TryClaimAndFetchGroupAsync(dbA, CancellationToken.None);
        Assert.That(claimedA, Is.Not.Null);
        Assert.That(claimedA!.Value.Group?.Id, Is.EqualTo(GroupAId));

        // Simulate crash/failure before commit.
        await txA.RollbackAsync();

        await using var scopeB = _provider.CreateAsyncScope();
        var dbB = scopeB.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txB = await dbB.Database.BeginTransactionAsync();

        var claimedB = await lockProvider.TryClaimAndFetchGroupAsync(dbB, CancellationToken.None);
        Assert.That(claimedB, Is.Not.Null, "Another worker should reclaim the shard after rollback");
        Assert.That(claimedB!.Value.Group?.Id, Is.EqualTo(GroupAId));

        await txB.RollbackAsync();
    }

    [Test]
    public async Task LockProvider_DoesNotDoubleClaim_WhenTwoConnectionsRace()
    {
        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            db.OutboxLocks.Add(new OutboxLock { Shard = 0 });
            db.OutboxGroups.Add(new OutboxGroup { Id = GroupAId, Shard = 0 });
            await db.SaveChangesAsync();
        }

        var lockProvider = new SqlServerOutboxLockProvider<TestDbContext>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var scopeA = _provider.CreateAsyncScope();
        await using var scopeB = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<TestDbContext>();
        var dbB = scopeB.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txA = await dbA.Database.BeginTransactionAsync();
        await using var txB = await dbB.Database.BeginTransactionAsync();

        async Task<(int Shard, OutboxGroup? Group)?> ClaimAsync(TestDbContext db)
        {
            await gate.Task;
            return await lockProvider.TryClaimAndFetchGroupAsync(db, CancellationToken.None);
        }

        var claimTaskA = ClaimAsync(dbA);
        var claimTaskB = ClaimAsync(dbB);
        gate.SetResult();

        await Task.WhenAll(claimTaskA, claimTaskB);

        var successfulClaimCount = new[] { claimTaskA.Result, claimTaskB.Result }
            .Count(c => c is not null && c.Value.Group is not null);
        Assert.That(successfulClaimCount, Is.EqualTo(1), "Only one connection may claim a given shard at a time");

        await txB.RollbackAsync();
        await txA.RollbackAsync();
    }
}
