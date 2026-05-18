using System.Text.Json;
using InterBus.Sagas;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Sagas;

/// <summary>
/// EF Core implementation of <see cref="ISagaRepository{TSaga, TState}"/> that persists saga
/// state to a relational database via a shared <typeparamref name="TDbContext"/>.
/// <para>
/// Saga state is stored as a JSON column in the <see cref="SagaStateEntry"/> table.
/// Multiple saga types can share the same <typeparamref name="TDbContext"/>; each type is
/// distinguished by a <c>SagaType</c> discriminator column derived from the fully-qualified
/// name of <typeparamref name="TSaga"/>.
/// </para>
/// <para>
/// <see cref="SaveAsync"/> persists the state and calls
/// <see cref="DbContext.SaveChangesAsync(CancellationToken)"/> so that the saga state is
/// committed atomically with any other pending changes tracked by the context in the same
/// consume scope.
/// </para>
/// </summary>
/// <typeparam name="TSaga">The saga type.</typeparam>
/// <typeparam name="TState">The saga state type.</typeparam>
/// <typeparam name="TDbContext">The application's <see cref="DbContext"/>.</typeparam>
public sealed class EfCoreSagaRepository<TSaga, TState, TDbContext>(
    TDbContext dbContext,
    ILockProvider lockProvider)
    : ISagaRepository<TSaga, TState>
    where TSaga : class, ISaga<TState>
    where TState : class, new()
    where TDbContext : DbContext
{
    private static readonly string SagaTypeName =
        typeof(TSaga).FullName ?? typeof(TSaga).Name;

    /// <inheritdoc/>
    public Task<IAsyncDisposable> AcquireLockAsync(string correlationId,
        CancellationToken cancellationToken = default)
    {
        var lockKey = $"{SagaTypeName}:{correlationId}";
        return lockProvider.AcquireLockAsync(lockKey, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<SagaEntry<TState>?> GetAsync(string correlationId,
        CancellationToken cancellationToken = default)
    {
        var row = await dbContext.Set<SagaStateEntry>()
            .FirstOrDefaultAsync(
                e => e.CorrelationId == correlationId && e.SagaType == SagaTypeName,
                cancellationToken);

        if (row is null)
            return null;

        // Lazy expiry: treat expired entries as absent and clean them up.
        if (row.ExpiresAtUtc.HasValue && row.ExpiresAtUtc.Value <= DateTimeOffset.UtcNow)
        {
            dbContext.Set<SagaStateEntry>().Remove(row);
            await dbContext.SaveChangesAsync(cancellationToken);
            return null;
        }

        var state = JsonSerializer.Deserialize<TState>(row.StateJson) ?? new TState();
        return new SagaEntry<TState>(state, row.ExpiresAtUtc) { IsCompleted = row.IsCompleted };
    }

    /// <inheritdoc/>
    public async Task SaveAsync(string correlationId, SagaEntry<TState> entry,
        CancellationToken cancellationToken = default)
    {
        var stateJson = JsonSerializer.Serialize(entry.State);

        // Check EF Core's local identity map first to avoid a redundant DB round-trip;
        // GetAsync will have loaded and tracked the entity if it already existed.
        var existing = dbContext.Set<SagaStateEntry>().Local
            .FirstOrDefault(e => e.CorrelationId == correlationId && e.SagaType == SagaTypeName);

        if (existing is not null)
        {
            existing.StateJson = stateJson;
            existing.IsCompleted = entry.IsCompleted;
            existing.ExpiresAtUtc = entry.ExpiresAtUtc;
        }
        else
        {
            dbContext.Set<SagaStateEntry>().Add(new SagaStateEntry
            {
                CorrelationId = correlationId,
                SagaType = SagaTypeName,
                StateJson = stateJson,
                IsCompleted = entry.IsCompleted,
                ExpiresAtUtc = entry.ExpiresAtUtc
            });
        }

        await dbContext.SaveChangesAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public async Task DeleteAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        await dbContext.Set<SagaStateEntry>()
            .Where(e => e.CorrelationId == correlationId && e.SagaType == SagaTypeName)
            .ExecuteDeleteAsync(cancellationToken);
    }
}
