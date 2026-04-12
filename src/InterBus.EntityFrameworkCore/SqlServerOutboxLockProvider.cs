using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace InterBus.EntityFrameworkCore;

/// <summary>
/// <see cref="IOutboxLockProvider{TDbContext}"/> for SQL Server databases. Uses
/// <c>UPDLOCK, READPAST</c> to atomically select and lock one outbox group row,
/// then reads all messages in that group.
/// </summary>
public sealed class SqlServerOutboxLockProvider<TDbContext> : IOutboxLockProvider<TDbContext>
    where TDbContext : DbContext, IOutboxDbContext
{
    public async Task<OutboxGroup?> TryFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken)
    {
        var (groupsFqn, groupIdCol) = GetTableInfo(context.OutboxGroups.EntityType, nameof(OutboxGroup.Id));
        var (_, visibleAtCol) = GetTableInfo(context.OutboxGroups.EntityType, nameof(OutboxGroup.VisibleAt));

        // Lock one oldest eligible group row; empty groups are still claimable and will be cleaned up.
        var groupSql = $"""
                        SELECT TOP (1) g.*
                        FROM {groupsFqn} g WITH (UPDLOCK, READPAST)
                        WHERE (g.{visibleAtCol} IS NULL OR g.{visibleAtCol} <= GETUTCDATE())
                        ORDER BY g.{groupIdCol}
                        """;

        return await context.OutboxGroups
            .FromSqlRaw(groupSql)
            .Include(g => g.Messages.OrderBy(m => m.Id))
            .FirstOrDefaultAsync(cancellationToken);
    }

    private static (string fqn, string keyCol) GetTableInfo(
        IEntityType entityType,
        string keyProperty)
    {
        var tableName = entityType.GetTableName() ?? entityType.ShortName();
        var schemaName = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schemaName);

        var keyCol = Quote(
            entityType.FindProperty(keyProperty)!.GetColumnName(storeObject)
            ?? keyProperty);

        var fqn = schemaName is not null
            ? $"{Quote(schemaName)}.{Quote(tableName)}"
            : Quote(tableName);

        return (fqn, keyCol);
    }

    private static string Quote(string name) =>
        $"[{name.Replace("]", "]]")}]";
}
