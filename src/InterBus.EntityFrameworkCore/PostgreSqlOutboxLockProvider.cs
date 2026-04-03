using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace InterBus.EntityFrameworkCore;

/// <summary>
/// <see cref="IOutboxLockProvider"/> for PostgreSQL databases. Uses a CTE with
/// <c>FOR UPDATE SKIP LOCKED</c> to atomically select the next pending group and lock
/// all its rows within the caller's transaction. Concurrent processor instances skip
/// locked groups and move on to the next available one.
/// </summary>
public sealed class PostgreSqlOutboxLockProvider : IOutboxLockProvider
{
    public Task<IReadOnlyList<OutboxMessage>> TryFetchGroupAsync(
        DbContext context,
        CancellationToken cancellationToken)
    {
        var (fqn, groupIdCol, createdAtCol) = GetTableInfo(context, '"', '"');

        // Identify the first unlocked group (SKIP LOCKED skips groups held by another
        // transaction), then lock all messages in that group FOR UPDATE.
        var sql = $"""
            WITH first_group AS (
                SELECT {groupIdCol}
                FROM {fqn}
                ORDER BY {createdAtCol}
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            SELECT om.*
            FROM {fqn} om
            INNER JOIN first_group fg ON om.{groupIdCol} = fg.{groupIdCol}
            ORDER BY om.{createdAtCol}
            FOR UPDATE
            """;

        return context.Set<OutboxMessage>()
            .FromSqlRaw(sql)
            .OrderBy(m => m.CreatedAt)
            .ToListAsync(cancellationToken)
            .ContinueWith(t => (IReadOnlyList<OutboxMessage>)t.Result, cancellationToken,
                TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private static (string fqn, string groupIdCol, string createdAtCol) GetTableInfo(
        DbContext context, char openQuote, char closeQuote)
    {
        var entityType = context.Model.FindEntityType(typeof(OutboxMessage))!;
        var tableName = entityType.GetTableName() ?? "OutboxMessages";
        var schemaName = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schemaName);

        var groupIdCol = Quote(
            entityType.FindProperty(nameof(OutboxMessage.GroupId))!.GetColumnName(storeObject)
            ?? nameof(OutboxMessage.GroupId),
            openQuote, closeQuote);
        var createdAtCol = Quote(
            entityType.FindProperty(nameof(OutboxMessage.CreatedAt))!.GetColumnName(storeObject)
            ?? nameof(OutboxMessage.CreatedAt),
            openQuote, closeQuote);

        var fqn = schemaName is not null
            ? $"{Quote(schemaName, openQuote, closeQuote)}.{Quote(tableName, openQuote, closeQuote)}"
            : Quote(tableName, openQuote, closeQuote);

        return (fqn, groupIdCol, createdAtCol);
    }

    private static string Quote(string name, char open, char close) =>
        $"{open}{name.Replace(close.ToString(), $"{close}{close}")}{close}";
}
