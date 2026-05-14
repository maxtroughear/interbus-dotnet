namespace InterBus.Outbox;

internal sealed class InMemoryOutboxConsumeState
{
    public List<InMemoryOutboxMessage> Messages = [];
}
