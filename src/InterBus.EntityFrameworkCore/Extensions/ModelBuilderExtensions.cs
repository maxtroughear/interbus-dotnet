using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the InterBus outbox entity mapping.</summary>
public static class ModelBuilderExtensions
{
    /// <summary>
    /// Registers the <see cref="OutboxGroup"/> and <see cref="OutboxMessage"/> entity types
    /// and applies recommended constraints/indexes for efficient outbox polling. Call this inside your
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

        modelBuilder.Entity<OutboxGroup>(b =>
        {
            b.HasKey(g => g.Id);
            b.Property(g => g.Id).ValueGeneratedNever();
            b.Property(g => g.CreatedAt).IsRequired();
            
            b.HasMany<OutboxMessage>()
                .WithOne(m => m.Group)
                .HasForeignKey(m => m.GroupId)
                .IsRequired()
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<OutboxMessage>(b =>
        {
            b.HasKey(m => m.Id);
            // Client-side Guid; no auto-generation needed.
            b.Property(m => m.Id).ValueGeneratedNever();
            b.Property(m => m.GroupId).IsRequired();
            b.Property(m => m.Destination).IsRequired().HasMaxLength(2048);
            b.Property(m => m.Payload).IsRequired();
            b.Property(m => m.HeadersJson).IsRequired();
            b.Property(m => m.CreatedAt).IsRequired();

            // Supports provider queries: fetch oldest claimable group, then read messages.
            b.HasIndex(m => new { m.GroupId, m.CreatedAt });
        });

        return modelBuilder;
    }
}
