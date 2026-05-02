using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace InterBus.EntityFrameworkCore.Sagas;

/// <summary>
/// <see cref="ISagaLockProvider{TDbContext}"/> for PostgreSQL databases.
/// Uses <c>pg_advisory_lock</c> (session-scoped) to acquire an exclusive lock keyed by the
/// saga type and correlation ID, preventing concurrent processing of the same saga instance
/// across multiple application nodes.
/// </summary>
public sealed class PostgreSqlSagaLockProvider<TDbContext> : ISagaLockProvider<TDbContext>
    where TDbContext : DbContext
{
    /// <inheritdoc/>
    public async Task<IAsyncDisposable> AcquireLockAsync(TDbContext context, string lockKey,
        CancellationToken cancellationToken = default)
    {
        // Keep the connection open for the duration of the advisory lock.
        // EF Core tracks a reference count; each OpenConnection call must be paired with
        // a CloseConnection call.
        await context.Database.OpenConnectionAsync(cancellationToken);

        try
        {
            var connection = context.Database.GetDbConnection();

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT pg_advisory_lock(hashtextextended(@lockKey, 0))";

            var param = cmd.CreateParameter();
            param.ParameterName = "lockKey";
            param.Value = lockKey;
            cmd.Parameters.Add(param);

            await cmd.ExecuteNonQueryAsync(cancellationToken);

            return new AdvisoryLockReleaser(context, lockKey);
        }
        catch
        {
            // If the advisory lock acquisition fails, release our connection reference.
            context.Database.CloseConnection();
            throw;
        }
    }

    private sealed class AdvisoryLockReleaser(TDbContext context, string lockKey) : IAsyncDisposable
    {
        private int _disposed;

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            var connection = context.Database.GetDbConnection();

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT pg_advisory_unlock(hashtextextended(@lockKey, 0))";

            var param = cmd.CreateParameter();
            param.ParameterName = "lockKey";
            param.Value = lockKey;
            cmd.Parameters.Add(param);

            try
            {
                await cmd.ExecuteNonQueryAsync();
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }
    }
}
