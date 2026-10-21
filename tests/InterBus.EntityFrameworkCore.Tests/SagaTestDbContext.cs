using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Sagas;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Tests;

public sealed class SagaTestDbContext(DbContextOptions<SagaTestDbContext> options) : DbContext(options)
{
    public DbSet<SagaStateEntry> SagaStateEntries => Set<SagaStateEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddInterBusSagas();
}
