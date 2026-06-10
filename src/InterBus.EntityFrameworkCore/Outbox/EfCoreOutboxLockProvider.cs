using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Default <see cref="IOutboxLockProvider{TDbContext}"/> that uses plain EF Core LINQ queries with no
/// database-level row locking. Safe for single-instance deployments and integration tests.
/// Processes groups in strict FIFO order (oldest <see cref="OutboxGroup.Id"/> first) globally.
/// For multi-instance deployments use <see cref="PostgreSqlOutboxLockProvider{TDbContext}"/> or
/// <see cref="SqlServerOutboxLockProvider{TDbContext}"/>.
/// </summary>
internal sealed class EfCoreOutboxLockProvider<TDbContext> : IOutboxLockProvider<TDbContext>
    where TDbContext : DbContext
{
    public async Task<(int Shard, OutboxGroup Group)?> TryClaimAndFetchGroupAsync(
        TDbContext context,
        CancellationToken cancellationToken)
    {
        var group = await context.Set<OutboxGroup>()
            .OrderBy(g => g.Id)
            .Include(g => g.Messages.OrderBy(m => m.Id))
            .FirstOrDefaultAsync(cancellationToken);

        return group is null ? null : (group.Shard, group);
    }
}
