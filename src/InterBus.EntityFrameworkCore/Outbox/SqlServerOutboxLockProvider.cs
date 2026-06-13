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
            await context.Database.ExecuteSqlRawAsync(
                $"""
                 IF NOT EXISTS (SELECT 1 FROM {locksFqn} WITH (UPDLOCK, HOLDLOCK) WHERE {shardCol} = {i})
                     INSERT INTO {locksFqn} ({shardCol}) VALUES ({i})
                 """,
                cancellationToken);
        }
    }

    /// <inheritdoc/>
    public async Task<(int Shard, OutboxGroup Group)?> TryClaimAndFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken,
        int preferredStartShard = 0)
    {
        var (locksFqn, locksShardCol) = GetTableInfo(context.Set<OutboxLock>().EntityType, nameof(OutboxLock.Shard));
        var (groupsFqn, groupsShardCol) = GetTableInfo(context.Set<OutboxGroup>().EntityType, nameof(OutboxGroup.Shard));
        
        var startShard = Math.Max(0, preferredStartShard);
        // Use a recursive CTE to generate candidate shards in preference order (starting from preferredStartShard,
        // wrapping around). This avoids the ORDER BY + TOP causing SQL Server to lock multiple rows before READPAST filters them.
        // The CTE generates the candidate list without locks, then we join and lock only the row we actually try to claim.
        var lockSql = $$"""
                        WITH candidate_shards AS (
                            SELECT {{startShard}} as shard_num, 0 as depth
                            UNION ALL
                            SELECT (shard_num + 1) % (SELECT COUNT(*) FROM {{locksFqn}}) as shard_num, depth + 1
                            FROM candidate_shards
                            WHERE depth < (SELECT COUNT(*) FROM {{locksFqn}})
                        )
                        SELECT TOP (1) l.*
                        FROM candidate_shards cs
                        INNER JOIN {{locksFqn}} l WITH (ROWLOCK, UPDLOCK, READPAST) ON l.{{locksShardCol}} = cs.shard_num
                        WHERE EXISTS (SELECT 1 FROM {{groupsFqn}} g WHERE g.{{groupsShardCol}} = l.{{locksShardCol}})
                        ORDER BY cs.depth
                        """;

        var claimedLock = await context.Set<OutboxLock>()
            .FromSqlRaw(lockSql)
            .FirstOrDefaultAsync(cancellationToken);

        if (claimedLock is null)
            return null;

        // Fetch the oldest group in the claimed shard (FIFO within the shard).
        var group = await context.Set<OutboxGroup>()
            .Where(g => g.Shard == claimedLock.Shard)
            .OrderBy(g => g.Id)
            .Include(g => g.Messages.OrderBy(m => m.Id))
            .FirstOrDefaultAsync(cancellationToken);

        return group is null ? null : (claimedLock.Shard, group);
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
