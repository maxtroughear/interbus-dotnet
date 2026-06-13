using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Outbox;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options)
    : DbContext(options)
{
    public DbSet<OutboxGroup> OutboxGroups => Set<OutboxGroup>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<OutboxLock> OutboxLocks => Set<OutboxLock>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddInterBusOutbox();
}
