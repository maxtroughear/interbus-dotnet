using InterBus.EntityFrameworkCore.Sagas;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the InterBus saga state entity mapping</summary>
public static class EfCoreSagaModelBuilderExtensions
{
    /// <summary>
    /// Registers the <see cref="SagaStateEntry"/> entity type and applies recommended
    /// constraints and indexes for saga persistence. Call this inside your
    /// <c>DbContext.OnModelCreating</c> override:
    /// <code>
    /// protected override void OnModelCreating(ModelBuilder modelBuilder)
    /// {
    ///     modelBuilder.AddInterBusSagas();
    /// }
    /// </code>
    /// </summary>
    public static ModelBuilder AddInterBusSagas(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<SagaStateEntry>(b =>
        {
            b.HasKey(e => new { e.CorrelationId, e.SagaType });
            b.Property(e => e.CorrelationId).IsRequired().HasMaxLength(500);
            b.Property(e => e.SagaType).IsRequired().HasMaxLength(500);
            b.Property(e => e.StateJson).IsRequired();
            b.Property(e => e.IsCompleted).IsRequired();
            b.Property(e => e.ExpiresAtUtc);

            b.HasIndex(e => new { e.SagaType, e.ExpiresAtUtc });
        });

        return modelBuilder;
    }
}
