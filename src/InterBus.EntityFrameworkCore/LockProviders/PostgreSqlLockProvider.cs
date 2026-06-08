using System.IO.Hashing;
using System.Text;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.LockProviders;

/// <summary>
/// <see cref="ILockProvider"/> for PostgreSQL databases.
/// Uses <c>pg_advisory_lock</c> (session-scoped) to acquire an exclusive lock keyed by the
/// saga type and correlation ID, preventing concurrent processing of the same saga instance
/// across multiple application nodes.
/// </summary>
public sealed class PostgreSqlLockProvider<TDbContext>(TDbContext context) : ILockProvider
    where TDbContext : DbContext
{
    /// <inheritdoc/>
    public async Task<IAsyncDisposable> AcquireLockAsync(string lockKey, CancellationToken cancellationToken = default)
    {
        var hashedLockKey = (long)XxHash3.HashToUInt64(Encoding.UTF8.GetBytes(lockKey));

        var locked =
            await context.Database.ExecuteSqlAsync($"SELECT pg_advisory_xact_lock({hashedLockKey})", cancellationToken);

        return locked == 0
            ? throw new InvalidOperationException("Failed to acquire advisory lock.")
            : AdvisoryLockReleaser.Instance;
    }

    private sealed class AdvisoryLockReleaser : IAsyncDisposable
    {
        public static readonly AdvisoryLockReleaser Instance = new();

        // Released as part of transaction
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
