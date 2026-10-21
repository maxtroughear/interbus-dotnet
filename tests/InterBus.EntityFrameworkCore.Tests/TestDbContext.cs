using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Outbox;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    public DbSet<InboxState> InboxStates => Set<InboxState>();
    public DbSet<OutboxLock> OutboxLocks => Set<OutboxLock>();
    public DbSet<OutboxGroup> OutboxGroups => Set<OutboxGroup>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddInterBusOutbox();
}
