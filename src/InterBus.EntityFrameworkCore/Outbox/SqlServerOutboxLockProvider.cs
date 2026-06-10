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
    // Tracks where round-robin shard selection should start. Updated after each successful claim
    // so successive iterations prefer the next shard, distributing work across processors.
    private volatile int _nextShard;
    private int _shardCount = 1;

    /// <inheritdoc/>
    public async Task EnsureShardLocksAsync(TDbContext context, int shardCount, CancellationToken cancellationToken)
    {
        _shardCount = Math.Max(1, shardCount);
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
        CancellationToken cancellationToken)
    {
        var (locksFqn, locksShardCol) = GetTableInfo(context.Set<OutboxLock>().EntityType, nameof(OutboxLock.Shard));
        var (groupsFqn, groupsShardCol) = GetTableInfo(context.Set<OutboxGroup>().EntityType, nameof(OutboxGroup.Shard));

        // Round-robin: prefer shards >= _nextShard so we rotate evenly instead of always
        // grabbing the lowest numbered shard. UPDLOCK, READPAST ensures concurrent processors
        // claim different shards.
        var startShard = _nextShard;
        var lockSql = $"""
                       SELECT TOP (1) l.*
                       FROM {locksFqn} l WITH (UPDLOCK, READPAST)
                       WHERE EXISTS (SELECT 1 FROM {groupsFqn} g WHERE g.{groupsShardCol} = l.{locksShardCol})
                       ORDER BY CASE WHEN l.{locksShardCol} >= {startShard} THEN 0 ELSE 1 END, l.{locksShardCol}
                       """;

        var claimedLock = await context.Set<OutboxLock>()
            .FromSqlRaw(lockSql)
            .FirstOrDefaultAsync(cancellationToken);

        if (claimedLock is null)
            return null;

        // Advance past the claimed shard for the next call; mod wraps back to 0 after the last shard.
        _nextShard = (claimedLock.Shard + 1) % _shardCount;

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
