using System.IO.Hashing;
using System.Text;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.LockProviders;

/// <summary>
/// <see cref="ILockProvider"/> for SQL Server databases.
/// Uses <c>sp_getapplock</c> (transaction-scoped) to acquire an exclusive application lock keyed
/// by the saga type and correlation ID, preventing concurrent processing of the same saga
/// instance across multiple application nodes.
/// <para>
/// Must be called within an active database transaction; the lock is released automatically
/// when that transaction commits or rolls back.
/// </para>
/// </summary>
public sealed class SqlServerLockProvider<TDbContext>(TDbContext context) : ILockProvider
    where TDbContext : DbContext
{
    /// <summary>
    /// Returns a fixed-length hex string that uniquely represents <paramref name="lockKey"/>
    /// for use as a <c>sp_getapplock</c> resource name (limit: 255 characters)
    /// </summary>
    private static string HashLockKey(string lockKey)
    {
        var hash = XxHash3.Hash(Encoding.UTF8.GetBytes(lockKey));
        return Convert.ToHexString(hash);
    }

    /// <inheritdoc/>
    public async Task<IAsyncDisposable> AcquireLockAsync(string lockKey, CancellationToken cancellationToken = default)
    {
        var resource = HashLockKey(lockKey);

        // @LockTimeout = -1 waits indefinitely. @LockOwner = 'Transaction' ties the lock to
        // the active transaction; it is released automatically on commit or rollback.
        await context.Database.ExecuteSqlAsync(
            $"EXEC sp_getapplock @Resource = {resource}, @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = -1",
            cancellationToken);

        return AppLockReleaser.Instance;
    }

    private sealed class AppLockReleaser : IAsyncDisposable
    {
        public static readonly AppLockReleaser Instance = new();

        // Lock is released as part of the transaction
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
