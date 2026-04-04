using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.MsSql;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

/// <summary>
/// Verifies that <see cref="SqlServerOutboxLockProvider{TDbContext}"/> correctly implements
/// <c>UPDLOCK, READPAST</c> semantics: a group that is already locked by one database
/// connection is skipped (not double-claimed) by a concurrent connection.
/// </summary>
public class OutboxSqlServerLockingTests
{
    // These GUIDs are ordered A < B in both PostgreSQL (lexicographic UUID order)
    // and SQL Server (last-6-bytes-first uniqueidentifier order) so the lock provider
    // always claims A before B.
    private static readonly Guid GroupAId = Guid.Parse("11111111-1111-7000-8000-100000000001");
    private static readonly Guid GroupBId = Guid.Parse("22222222-2222-7000-8000-200000000002");

    // Separate container so these locking tests are fully isolated from
    // OutboxSqlServerIntegrationTests (which runs a background processor).
    private static readonly MsSqlContainer Container =
        new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    private ServiceProvider _provider = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp() => await Container.StartAsync();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await Container.StopAsync();
        await Container.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<TestDbContext>(o => o.UseSqlServer(Container.GetConnectionString()));

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
            // Remove any groups inserted by this test so the next test starts clean.
            await using var scope = _provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.OutboxGroups
                .Where(g => g.Id == GroupAId || g.Id == GroupBId)
                .ExecuteDeleteAsync();
        }
        finally
        {
            await _provider.DisposeAsync();
        }
    }

    /// <summary>
    /// When the only group in the outbox is already locked by an open transaction on
    /// connection A, connection B must return <c>null</c> (no unlocked groups available).
    /// </summary>
    [Test]
    public async Task LockProvider_ReturnsNull_WhenOnlyGroupIsLocked()
    {
        // Arrange – insert one group
        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            db.OutboxGroups.Add(new OutboxGroup { Id = GroupAId });
            await db.SaveChangesAsync();
        }

        var lockProvider = new SqlServerOutboxLockProvider<TestDbContext>();

        // Connection A: open a transaction and lock the only group.
        await using var scopeA = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txA = await dbA.Database.BeginTransactionAsync();

        var claimedGroup = await lockProvider.TryFetchGroupAsync(dbA, CancellationToken.None);
        Assert.That(claimedGroup, Is.Not.Null, "Connection A should claim the only available group");
        Assert.That(claimedGroup!.Id, Is.EqualTo(GroupAId));

        // Connection B: while A holds the lock, there are no unlocked groups left.
        await using var scopeB = _provider.CreateAsyncScope();
        var dbB = scopeB.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txB = await dbB.Database.BeginTransactionAsync();

        var result = await lockProvider.TryFetchGroupAsync(dbB, CancellationToken.None);

        Assert.That(result, Is.Null,
            "Connection B must return null because the only group is locked by connection A (UPDLOCK, READPAST)");

        await txB.RollbackAsync();
        await txA.RollbackAsync();
    }

    /// <summary>
    /// When two groups exist and one is locked by connection A, connection B must skip the
    /// locked group and successfully claim the other one.
    /// </summary>
    [Test]
    public async Task LockProvider_SkipsLockedGroup_WhenAnotherGroupIsAvailable()
    {
        // Arrange – insert two groups; GroupAId < GroupBId so the lock provider claims A first.
        await using (var scope = _provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            db.OutboxGroups.AddRange(
                new OutboxGroup { Id = GroupAId },
                new OutboxGroup { Id = GroupBId });
            await db.SaveChangesAsync();
        }

        var lockProvider = new SqlServerOutboxLockProvider<TestDbContext>();

        // Connection A: lock groupA (lowest ID, so claimed first).
        await using var scopeA = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txA = await dbA.Database.BeginTransactionAsync();

        var claimedGroup = await lockProvider.TryFetchGroupAsync(dbA, CancellationToken.None);
        Assert.That(claimedGroup, Is.Not.Null, "Connection A should claim groupA first");
        Assert.That(claimedGroup!.Id, Is.EqualTo(GroupAId),
            "GroupA (lower ID) should be claimed by connection A");

        // Connection B: groupA is locked, so it should skip A and claim groupB.
        await using var scopeB = _provider.CreateAsyncScope();
        var dbB = scopeB.ServiceProvider.GetRequiredService<TestDbContext>();
        await using var txB = await dbB.Database.BeginTransactionAsync();

        var skippedToGroup = await lockProvider.TryFetchGroupAsync(dbB, CancellationToken.None);

        Assert.That(skippedToGroup, Is.Not.Null,
            "Connection B should find groupB even though groupA is locked");
        Assert.That(skippedToGroup!.Id, Is.EqualTo(GroupBId),
            "Connection B must skip the locked groupA and claim groupB (UPDLOCK, READPAST)");

        await txB.RollbackAsync();
        await txA.RollbackAsync();
    }
}
