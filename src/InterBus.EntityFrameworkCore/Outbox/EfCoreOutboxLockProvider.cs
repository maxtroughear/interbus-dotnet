using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Default <see cref="IOutboxLockProvider{TDbContext}"/> that uses plain EF Core LINQ queries with no
/// database-level row locking. Safe for single-instance deployments and integration tests.
/// For multi-instance deployments use <see cref="PostgreSqlOutboxLockProvider{TDbContext}"/> or
/// <see cref="SqlServerOutboxLockProvider{TDbContext}"/>.
/// </summary>
internal sealed class EfCoreOutboxLockProvider<TDbContext> : IOutboxLockProvider<TDbContext>
    where TDbContext : DbContext
{
    public async Task<OutboxGroup?> TryFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        return await context.Set<OutboxGroup>()
            .Where(g => g.VisibleAt <= now)
            .OrderBy(g => g.VisibleAt)
            .ThenBy(g => g.Id)
            .Include(g => g.Messages.OrderBy(m => m.Id))
            .FirstOrDefaultAsync(cancellationToken);
    }
}
