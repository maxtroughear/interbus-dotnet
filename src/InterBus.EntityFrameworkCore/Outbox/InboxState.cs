namespace InterBus.EntityFrameworkCore.Outbox;

public enum InboxProcessingState
{
    PendingConsumer = 0,
    PendingOutbox = 1,
    Completed = 2
}

public sealed class InboxState
{
    public required string ConsumerName { get; init; }
    public required string MessageId { get; init; }
    public required InboxProcessingState State { get; set; }
    public Guid? OutboxGroupId { get; set; }
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
