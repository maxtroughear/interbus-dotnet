using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace InterBus.EntityFrameworkCore.Internal;

/// <summary>
/// Default <see cref="IOutboxLockProvider"/> that uses plain EF Core LINQ queries with no
/// database-level row locking. Safe for single-instance deployments and integration tests.
/// For multi-instance deployments use <see cref="PostgreSqlOutboxLockProvider"/> or
/// <see cref="SqlServerOutboxLockProvider"/>.
/// </summary>
internal sealed class EFCoreOutboxLockProvider : IOutboxLockProvider
{
    public async Task<IReadOnlyList<OutboxMessage>> TryFetchGroupAsync(
        DbContext context,
        CancellationToken cancellationToken)
    {
        var groupId = await context.Set<OutboxMessage>()
            .OrderBy(m => m.CreatedAt)
            .Select(m => (Guid?)m.GroupId)
            .FirstOrDefaultAsync(cancellationToken);

        if (groupId is null)
            return [];

        return await context.Set<OutboxMessage>()
            .Where(m => m.GroupId == groupId.Value)
            .OrderBy(m => m.CreatedAt)
            .ToListAsync(cancellationToken);
    }
}
