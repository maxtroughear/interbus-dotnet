using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// <see cref="IOutboxLockProvider{TDbContext}"/> for PostgreSQL databases. Uses
/// <c>FOR UPDATE SKIP LOCKED</c> to atomically select and lock the next outbox group row,
/// then reads all messages in that group.
/// </summary>
public sealed class PostgreSqlOutboxLockProvider<TDbContext> : IOutboxLockProvider<TDbContext>
    where TDbContext : DbContext
{
    public async Task<OutboxGroup?> TryFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken)
    {
        var (groupsFqn, idCol) = GetTableInfo(context.Set<OutboxGroup>().EntityType, nameof(OutboxGroup.Id));

        // Lock one oldest eligible group row; empty groups are still claimable and will be cleaned up.
        var groupSql = $"""
                        SELECT g.*
                        FROM {groupsFqn} g
                        ORDER BY g.{idCol}
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """;

        var group = await context.Set<OutboxGroup>()
            .FromSqlRaw(groupSql)
            .Include(g => g.Messages.OrderBy(m => m.Id))
            .FirstOrDefaultAsync(cancellationToken);

        return group;
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
