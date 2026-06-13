using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// <see cref="IOutboxLockProvider{TDbContext}"/> for PostgreSQL databases.
/// <para>
/// On startup, ensures one <see cref="OutboxLock"/> row exists per shard. At processing time,
/// uses <c>FOR UPDATE SKIP LOCKED</c> to atomically claim the oldest available shard that has
/// pending groups, then fetches the oldest <see cref="OutboxGroup"/> in that shard to guarantee
/// FIFO delivery within each shard. Multiple processor instances can work in parallel on
/// different shards without blocking one another.
/// </para>
/// </summary>
public sealed class PostgreSqlOutboxLockProvider<TDbContext> : IOutboxLockProvider<TDbContext>
    where TDbContext : DbContext
{
    /// <inheritdoc/>
    public async Task EnsureShardLocksAsync(TDbContext context, int shardCount, CancellationToken cancellationToken)
    {
        var (locksFqn, shardCol) = GetTableInfo(context.Set<OutboxLock>().EntityType, nameof(OutboxLock.Shard));

        for (var i = 0; i < shardCount; i++)
        {
            var insertLockSql = $"INSERT INTO {locksFqn} ({shardCol}) VALUES ({i}) ON CONFLICT ({shardCol}) DO NOTHING";
            
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
        var (groupsFqn, groupsShardCol) =
            GetTableInfo(context.Set<OutboxGroup>().EntityType, nameof(OutboxGroup.Shard));

        // Round-robin: prefer shards >= preferredStartShard so workers can rotate shard preference.
        // FOR UPDATE SKIP LOCKED ensures concurrent processors claim different shards.
        // PostgreSQL row locks apply correctly with ORDER BY: SKIP LOCKED skips locked rows before sorting,
        // so only unlocked rows are ordered, preventing lock escalation on small lock tables.
        var startShard = Math.Max(0, preferredStartShard);
        var lockSql = $"""
                       SELECT l.*
                       FROM {locksFqn} l
                       WHERE EXISTS (SELECT 1 FROM {groupsFqn} g WHERE g.{groupsShardCol} = l.{locksShardCol})
                       ORDER BY CASE WHEN l.{locksShardCol} >= {startShard} THEN 0 ELSE 1 END, l.{locksShardCol}
                       LIMIT 1
                       FOR UPDATE SKIP LOCKED
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
        $"\"{name.Replace("\"", "\"\"")}\"";
}
