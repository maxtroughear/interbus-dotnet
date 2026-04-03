namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Configuration options for the InterBus transactional outbox.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// How often the background processor polls the outbox table when there are no pending
    /// messages. Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of
    /// more frequent database queries when the outbox is empty.
    /// <para>
    /// The processor does not wait between iterations when messages are available — it
    /// immediately processes the next group after completing the current one.
    /// </para>
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The strategy used to lock a group of pending messages before forwarding them.
    /// Defaults to <see cref="Internal.EFCoreOutboxLockProvider"/>, which uses plain EF Core
    /// queries with no database-level row locking. Suitable for single-instance deployments
    /// and integration tests. For multi-instance deployments configure a provider that
    /// matches your database:
    /// <list type="bullet">
    ///   <item>PostgreSQL — <see cref="PostgreSqlOutboxLockProvider"/></item>
    ///   <item>SQL Server — <see cref="SqlServerOutboxLockProvider"/></item>
    /// </list>
    /// </summary>
    public IOutboxLockProvider LockProvider { get; set; } = new Internal.EFCoreOutboxLockProvider();
}
