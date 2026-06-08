using InterBus.Consuming;

namespace InterBus.InMemory.Outbox;

internal sealed class InMemoryOutboxConsumeMiddleware(InMemoryOutboxConsumeState consumeState) : IConsumeMiddleware
{
    private bool _consumeHasRun;

    public async Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        if (!_consumeHasRun)
        {
            consumeState.Messages = [];
            await next(context, cancellationToken);
        }

        _consumeHasRun = true;

        foreach (var message in consumeState.Messages)
        {
            if (message.Delivered)
                continue;

            await message.Next(message.Context, cancellationToken);
            message.Delivered = true;
        }
    }
}
