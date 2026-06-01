// ReSharper disable EntityFramework.ModelValidation.UnlimitedStringLength - set in ModelBuilder

namespace InterBus.EntityFrameworkCore.Outbox;

public enum InboxProcessingState
{
    PendingConsumer = 0,
    PendingOutbox = 1,
    Completed = 2
}

public sealed class InboxState(string consumerName, string messageId)
{
    public Guid Id { get; init; } = Guid.CreateVersion7();
    public string ConsumerName { get; } = consumerName;
    public string MessageId { get; } = messageId;
    public InboxProcessingState State { get; private set; } = InboxProcessingState.PendingConsumer;
    public DateTime CreatedAt { get; private set; } = DateTime.UtcNow;
    public DateTime? ConsumedAt { get; private set; }
    public DateTime? DeliveredAt { get; private set; }
    
    public ICollection<OutboxMessage> Messages { get; } = new List<OutboxMessage>();

    public void Consume()
    {
        State = InboxProcessingState.PendingOutbox;
        ConsumedAt = DateTime.UtcNow;
    }

    public void Complete()
    {
        State = InboxProcessingState.Completed;
        DeliveredAt = DateTime.UtcNow;
    }
}
