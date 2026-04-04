using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Marker interface that an application's <see cref="DbContext"/> must implement to participate
/// in the InterBus transactional outbox. Implement this interface and call
/// <c>modelBuilder.AddInterBusOutbox()</c> inside <c>OnModelCreating</c> to configure the
/// entity mapping.
/// </summary>
public interface IOutboxDbContext
{
    /// <summary>
    /// The outbox group/header table. One row represents one logical publish scope.
    /// Lock providers claim rows from this table to ensure only one processor instance
    /// handles a group at a time.
    /// </summary>
    DbSet<OutboxGroup> OutboxGroups { get; }

    /// <summary>
    /// The outbox messages table. Messages are written here by
    /// <see cref="InterBus.Middleware.IPublishMiddleware"/> and deleted once successfully
    /// forwarded to the transport by the background processor.
    /// </summary>
    DbSet<OutboxMessage> OutboxMessages { get; }
}
