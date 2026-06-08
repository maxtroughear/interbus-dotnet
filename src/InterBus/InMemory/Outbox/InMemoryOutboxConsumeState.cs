namespace InterBus.InMemory.Outbox;

internal sealed class InMemoryOutboxConsumeState
{
    public List<InMemoryOutboxMessage> Messages = [];
}
