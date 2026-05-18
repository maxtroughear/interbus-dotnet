using InterBus.EntityFrameworkCore.Outbox;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the InterBus outbox entity mapping.</summary>
public static class EfCoreOutboxModelBuilderExtensions
{
    /// <summary>
    /// Registers the <see cref="OutboxGroup"/>, <see cref="OutboxMessage"/>, and <see cref="InboxState"/> entity types
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
            b.Property(g => g.VisibleAt).IsRequired();

            b.HasIndex(g => new { g.VisibleAt, g.Id });

            b.HasMany<OutboxMessage>()
                .WithOne(m => m.Group)
                .HasForeignKey(m => m.GroupId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<OutboxMessage>(b =>
        {
            b.HasKey(m => m.Id);
            // Client-side Guid; no auto-generation needed.
            b.Property(m => m.Id).ValueGeneratedNever();
            b.Property(m => m.Destination).IsRequired().HasMaxLength(2048);
            b.Property(m => m.Payload).IsRequired();
            b.Property(m => m.HeadersJson).IsRequired().HasMaxLength(4096);
            b.Property(m => m.CreatedAt).IsRequired();

            b.HasIndex(m => new { m.GroupId, m.Id });
            b.HasIndex(m => new { m.InboxId, m.Id });
        });

        modelBuilder.Entity<InboxState>(b =>
        {
            b.HasKey(i => i.Id);
            b.Property(i => i.ConsumerName).HasMaxLength(1024);
            b.Property(i => i.MessageId).HasMaxLength(256);
            b.Property(i => i.State).IsRequired();
            b.Property(i => i.CreatedAt).IsRequired();

            b.HasIndex(i => i.CreatedAt);
            b.HasIndex(i => i.DeliveredAt);
            b.HasIndex(i => new { i.ConsumerName, i.MessageId }).IsUnique();
            
            b.HasMany<OutboxMessage>()
                .WithOne(m => m.Inbox)
                .HasForeignKey(m => m.InboxId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        return modelBuilder;
    }
}
