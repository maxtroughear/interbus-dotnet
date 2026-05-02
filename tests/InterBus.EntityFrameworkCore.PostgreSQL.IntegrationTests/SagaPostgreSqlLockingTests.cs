using InterBus.EntityFrameworkCore.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;

namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

/// <summary>
/// Verifies that <see cref="PostgreSqlSagaLockProvider{TDbContext}"/> correctly implements
/// <c>pg_advisory_lock</c> semantics: an advisory lock acquired by one session blocks
/// a concurrent session from acquiring the same lock until it is released.
/// </summary>
[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class SagaPostgreSqlLockingTests(string imageTag)
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();
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
        services.AddDbContext<SagaTestDbContext>(o => o.UseNpgsql(_container.GetConnectionString()));

        _provider = services.BuildServiceProvider();

        await using var scope = _provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<SagaTestDbContext>()
            .Database.EnsureCreatedAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        await _provider.DisposeAsync();
    }

    /// <summary>
    /// Session A holds a pg_advisory_lock on a key. Session B attempting to acquire the same
    /// lock via the lock provider must block until session A releases it.
    /// </summary>
    [Test]
    public async Task LockProvider_SecondSession_WaitsForFirstToRelease()
    {
        const string lockKey = "TestSaga:corr-lock-test";
        var lockProvider = new PostgreSqlSagaLockProvider<SagaTestDbContext>();

        // Session A acquires the lock.
        await using var scopeA = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<SagaTestDbContext>();
        var lockA = await lockProvider.AcquireLockAsync(dbA, lockKey);

        var sessionBStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sessionBReleased = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Session B tries to acquire the same lock in a background task.
        // It must block until session A releases.
        var sessionBTask = Task.Run(async () =>
        {
            await using var scopeB = _provider.CreateAsyncScope();
            var dbB = scopeB.ServiceProvider.GetRequiredService<SagaTestDbContext>();

            sessionBStarted.SetResult(true);

            // This call blocks until session A releases.
            var lockB = await lockProvider.AcquireLockAsync(dbB, lockKey);
            sessionBReleased.SetResult(true);
            await lockB.DisposeAsync();
        });

        // Wait for session B to be running but blocked.
        await sessionBStarted.Task;
        await Task.Delay(100); // Give session B time to reach the lock acquisition call.

        Assert.That(sessionBReleased.Task.IsCompleted, Is.False,
            "Session B must be blocked waiting for session A to release the lock");

        // Release session A's lock — session B should now proceed.
        await lockA.DisposeAsync();

        await Task.WhenAny(sessionBTask, Task.Delay(5000));

        Assert.That(sessionBReleased.Task.IsCompleted, Is.True,
            "Session B should have acquired the lock after session A released it");
    }

    /// <summary>
    /// Two sessions acquiring locks with different keys must not block each other.
    /// </summary>
    [Test]
    public async Task LockProvider_DifferentKeys_DoNotBlockEachOther()
    {
        var lockProvider = new PostgreSqlSagaLockProvider<SagaTestDbContext>();

        await using var scopeA = _provider.CreateAsyncScope();
        var dbA = scopeA.ServiceProvider.GetRequiredService<SagaTestDbContext>();
        var lockA = await lockProvider.AcquireLockAsync(dbA, "TestSaga:corr-key-a");

        await using var scopeB = _provider.CreateAsyncScope();
        var dbB = scopeB.ServiceProvider.GetRequiredService<SagaTestDbContext>();

        // Should not block because the key is different.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var lockB = await lockProvider.AcquireLockAsync(dbB, "TestSaga:corr-key-b", cts.Token);

        Assert.Pass("Session B acquired a different key lock without blocking");

        await lockB.DisposeAsync();
        await lockA.DisposeAsync();
    }
}
