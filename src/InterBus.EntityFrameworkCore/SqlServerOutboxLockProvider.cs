using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace InterBus.EntityFrameworkCore;

/// <summary>
/// <see cref="IOutboxLockProvider"/> for SQL Server databases. Uses a CTE with
/// <c>UPDLOCK, READPAST</c> hints to atomically select the next pending group and lock
/// all its rows within the caller's transaction. Concurrent processor instances skip
/// locked groups and move on to the next available one.
/// </summary>
public sealed class SqlServerOutboxLockProvider : IOutboxLockProvider
{
    public Task<IReadOnlyList<OutboxMessage>> TryFetchGroupAsync(
        DbContext context,
        CancellationToken cancellationToken)
    {
        var (fqn, groupIdCol, createdAtCol) = GetTableInfo(context);

        // READPAST skips rows locked by other transactions (equivalent to SKIP LOCKED).
        // UPDLOCK promotes the lock to an update lock, preventing phantom reads.
        var sql = $"""
            WITH first_group AS (
                SELECT {groupIdCol}
                FROM {fqn} WITH (UPDLOCK, READPAST)
                ORDER BY {createdAtCol}
                OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
            )
            SELECT om.*
            FROM {fqn} om WITH (UPDLOCK)
            INNER JOIN first_group fg ON om.{groupIdCol} = fg.{groupIdCol}
            ORDER BY om.{createdAtCol}
            """;

        return context.Set<OutboxMessage>()
            .FromSqlRaw(sql)
            .OrderBy(m => m.CreatedAt)
            .ToListAsync(cancellationToken)
            .ContinueWith(t => (IReadOnlyList<OutboxMessage>)t.Result, cancellationToken,
                TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private static (string fqn, string groupIdCol, string createdAtCol) GetTableInfo(
        DbContext context)
    {
        var entityType = context.Model.FindEntityType(typeof(OutboxMessage))!;
        var tableName = entityType.GetTableName() ?? "OutboxMessages";
        var schemaName = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schemaName);

        var groupIdCol = Quote(
            entityType.FindProperty(nameof(OutboxMessage.GroupId))!.GetColumnName(storeObject)
            ?? nameof(OutboxMessage.GroupId));
        var createdAtCol = Quote(
            entityType.FindProperty(nameof(OutboxMessage.CreatedAt))!.GetColumnName(storeObject)
            ?? nameof(OutboxMessage.CreatedAt));

        var fqn = schemaName is not null
            ? $"{Quote(schemaName)}.{Quote(tableName)}"
            : Quote(tableName);

        return (fqn, groupIdCol, createdAtCol);
    }

    private static string Quote(string name) =>
        $"[{name.Replace("]", "]]")}]";
}
