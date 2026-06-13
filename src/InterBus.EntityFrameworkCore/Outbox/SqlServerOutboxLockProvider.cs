using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// <see cref="IOutboxLockProvider{TDbContext}"/> for SQL Server databases.
/// <para>
/// On startup, ensures one <see cref="OutboxLock"/> row exists per shard. At processing time,
/// uses <c>UPDLOCK, READPAST</c> hints to atomically claim the oldest available shard that has
/// pending groups, then fetches the oldest <see cref="OutboxGroup"/> in that shard to guarantee
/// FIFO delivery within each shard. Multiple processor instances can work in parallel on
/// different shards without blocking one another.
/// </para>
/// </summary>
public sealed class SqlServerOutboxLockProvider<TDbContext> : IOutboxLockProvider<TDbContext>
    where TDbContext : DbContext
{
    /// <inheritdoc/>
    public async Task EnsureShardLocksAsync(TDbContext context, int shardCount, CancellationToken cancellationToken)
    {
        var (locksFqn, shardCol) = GetTableInfo(context.Set<OutboxLock>().EntityType, nameof(OutboxLock.Shard));

        for (var i = 0; i < shardCount; i++)
        {
            // UPDLOCK + HOLDLOCK on the NOT EXISTS check prevent the TOCTOU race where two
            // concurrent instances both see "not exists" and both attempt the INSERT.
            var insertLockSql = $"""
                                 IF NOT EXISTS (SELECT 1 FROM {locksFqn} WITH (UPDLOCK, HOLDLOCK) WHERE {shardCol} = {i})
                                     INSERT INTO {locksFqn} ({shardCol}) VALUES ({i})
                                 """;

            await context.Database.ExecuteSqlRawAsync(insertLockSql, cancellationToken);
        }
    }

    /// <inheritdoc/>
    public async Task<(int Shard, OutboxGroup? Group)?> TryClaimAndFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken,
        int preferredStartShard = 0)
    {
        var (locksFqn, locksShardCol) = GetTableInfo(context.Set<OutboxLock>().EntityType, nameof(OutboxLock.Shard));

        var startShard = Math.Max(0, preferredStartShard);

        var simpleLock = $$"""
                           SELECT TOP (1) l.*
                           FROM {{locksFqn}} l WITH (ROWLOCK, UPDLOCK, READPAST)
                           WHERE l.{{locksShardCol}} = {{startShard}}
                           """;

        var claimedLock = await context.Set<OutboxLock>()
            .FromSqlRaw(simpleLock)
            .FirstOrDefaultAsync(cancellationToken);

        // Return the shard here to tell the processor to loop back around immediately.
        // Locking the next shard will be done by the processor.
        if (claimedLock is null)
            return (preferredStartShard, null);

        // Fetch the oldest group in the claimed shard (FIFO within the shard).
        var group = await context.Set<OutboxGroup>()
            .Where(g => g.Shard == claimedLock.Shard)
            .OrderBy(g => g.Id)
            .Include(g => g.Messages.OrderBy(m => m.Id))
            .FirstOrDefaultAsync(cancellationToken);

        return (claimedLock.Shard, group);
    }

    private static (string fqn, string col) GetTableInfo(IEntityType entityType, string colProperty)
    {
        var tableName = entityType.GetTableName() ?? entityType.ShortName();
        var schemaName = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schemaName);

        var col = Quote(
            entityType.FindProperty(colProperty)!.GetColumnName(storeObject)
            ?? colProperty);

        var fqn = schemaName is not null
            ? $"{Quote(schemaName)}.{Quote(tableName)}"
            : Quote(tableName);

        return (fqn, col);
    }

    private static string Quote(string name) =>
        $"[{name.Replace("]", "]]")}]";
}
