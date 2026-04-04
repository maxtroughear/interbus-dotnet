using InterBus.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options)
    : DbContext(options), IOutboxDbContext
{
    public DbSet<OutboxGroup> OutboxGroups => Set<OutboxGroup>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddInterBusOutbox();
}
