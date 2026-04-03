using Microsoft.EntityFrameworkCore;

namespace InterBus.EFCore.Extensions;

/// <summary>Extension methods for configuring the InterBus outbox entity mapping.</summary>
public static class ModelBuilderExtensions
{
    /// <summary>
    /// Registers the <see cref="OutboxMessage"/> entity type and applies the recommended
    /// column types and index for efficient outbox polling. Call this inside your
    /// <c>DbContext.OnModelCreating</c> override:
    /// <code>
    /// protected override void OnModelCreating(ModelBuilder modelBuilder)
    /// {
    ///     modelBuilder.AddInterBusOutbox();
    /// }
    /// </code>
    /// </summary>
    public static ModelBuilder AddInterBusOutbox(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<OutboxMessage>(b =>
        {
            b.HasKey(m => m.Id);
            // Client-side Guid; no auto-generation needed.
            b.Property(m => m.Id).ValueGeneratedNever();
            b.Property(m => m.Destination).IsRequired().HasMaxLength(2048);
            b.Property(m => m.Payload).IsRequired();
            b.Property(m => m.HeadersJson).IsRequired();
            b.Property(m => m.CreatedAt).IsRequired();

            // Index that supports the background processor's polling query:
            // WHERE ProcessedAt IS NULL AND (VisibleAt IS NULL OR VisibleAt <= @now)
            // ORDER BY CreatedAt
            b.HasIndex(m => new { m.ProcessedAt, m.VisibleAt, m.CreatedAt });
        });

        return modelBuilder;
    }
}
