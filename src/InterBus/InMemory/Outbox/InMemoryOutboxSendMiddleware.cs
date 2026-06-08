using InterBus.Sending;

namespace InterBus.InMemory.Outbox;

internal sealed class InMemoryOutboxSendMiddleware(InMemoryOutboxConsumeState consumeState) : ISendMiddleware
{
    public Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        consumeState.Messages.Add(new InMemoryOutboxMessage(context, next));
        return Task.CompletedTask;
    }
}
