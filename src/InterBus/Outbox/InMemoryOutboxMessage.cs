using InterBus.Sending;

namespace InterBus.Outbox;

internal class InMemoryOutboxMessage(SendContext context, Func<SendContext, CancellationToken, Task> next)
{
    public SendContext Context { get; } = context;
    public Func<SendContext, CancellationToken, Task> Next { get; } = next;

    public bool Delivered { get; set; } = false;
}
