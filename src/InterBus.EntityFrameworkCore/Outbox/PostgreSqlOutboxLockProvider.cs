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
        var (groupsFqn, visibleAtCol, idCol) =
            GetTableInfo(context.Set<OutboxGroup>().EntityType, nameof(OutboxGroup.VisibleAt), nameof(OutboxGroup.Id));

        // Lock one oldest eligible group row; empty groups are still claimable and will be cleaned up.
        var groupSql = $"""
                        SELECT g.*
                        FROM {groupsFqn} g
                        WHERE g.{visibleAtCol} <= NOW()
                        ORDER BY g.{visibleAtCol}, g.{idCol}
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """;

        var group = await context.Set<OutboxGroup>()
            .FromSqlRaw(groupSql)
            .Include(g => g.Messages.OrderBy(m => m.Id))
            .FirstOrDefaultAsync(cancellationToken);

        return group;
    }

    private static (string fqn, string col1, string col2) GetTableInfo(IEntityType entityType, string colProperty1,
        string colProperty2)
    {
        var tableName = entityType.GetTableName() ?? entityType.ShortName();
        var schemaName = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schemaName);

        var col1 = Quote(
            entityType.FindProperty(colProperty1)!.GetColumnName(storeObject)
            ?? colProperty1);

        var col2 = Quote(
            entityType.FindProperty(colProperty2)!.GetColumnName(storeObject)
            ?? colProperty2);

        var fqn = schemaName is not null
            ? $"{Quote(schemaName)}.{Quote(tableName)}"
            : Quote(tableName);

        return (fqn, col1, col2);
    }

    private static string Quote(string name) =>
        $"\"{name.Replace("\"", "\"\"")}\"";
}
