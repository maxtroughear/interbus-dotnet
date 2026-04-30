using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Sagas;

/// <summary>
/// <see cref="ISagaLockProvider{TDbContext}"/> for SQL Server databases.
/// Uses <c>sp_getapplock</c> (session-scoped) to acquire an exclusive application lock keyed
/// by the saga type and correlation ID, preventing concurrent processing of the same saga
/// instance across multiple application nodes.
/// </summary>
public sealed class SqlServerSagaLockProvider<TDbContext> : ISagaLockProvider<TDbContext>
    where TDbContext : DbContext
{
    // sp_getapplock resource names are limited to 255 characters.
    private const int MaxResourceLength = 255;

    /// <inheritdoc/>
    public async Task<IAsyncDisposable> AcquireLockAsync(TDbContext context, string lockKey,
        CancellationToken cancellationToken = default)
    {
        // Truncate to 255 characters to satisfy sp_getapplock's resource name limit.
        var resource = lockKey.Length <= MaxResourceLength
            ? lockKey
            : lockKey[..MaxResourceLength];

        // Keep the connection open for the duration of the application lock.
        await context.Database.OpenConnectionAsync(cancellationToken);

        try
        {
            var connection = context.Database.GetDbConnection();

            await using var cmd = connection.CreateCommand();
            // @LockTimeout = -1 means wait indefinitely until the lock is granted.
            // @LockOwner = 'Session' ensures the lock survives the current transaction.
            cmd.CommandText = """
                              EXEC sp_getapplock
                                  @Resource   = @resource,
                                  @LockMode   = N'Exclusive',
                                  @LockOwner  = N'Session',
                                  @LockTimeout = -1
                              """;

            var param = cmd.CreateParameter();
            param.ParameterName = "resource";
            param.Value = resource;
            cmd.Parameters.Add(param);

            await cmd.ExecuteNonQueryAsync(cancellationToken);

            return new AppLockReleaser(context, resource);
        }
        catch
        {
            context.Database.CloseConnection();
            throw;
        }
    }

    private sealed class AppLockReleaser(TDbContext context, string resource) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            var connection = context.Database.GetDbConnection();

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "EXEC sp_releaseapplock @Resource = @resource, @LockOwner = N'Session'";

            var param = cmd.CreateParameter();
            param.ParameterName = "resource";
            param.Value = resource;
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
