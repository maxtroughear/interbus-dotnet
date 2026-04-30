using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public sealed class SagaTestDbContext(DbContextOptions<SagaTestDbContext> options)
    : DbContext(options)
{
    public DbSet<SagaStateEntry> SagaStateEntries => Set<SagaStateEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.AddInterBusSagas();
}
