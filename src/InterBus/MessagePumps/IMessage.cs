using InterBus.Dispatching;

namespace InterBus.MessagePumps;

public interface IMessage
{
    public Task Acknowledge(CancellationToken cancellationToken);
    public Task Abandon(CancellationToken cancellationToken);
    public Task DeadLetter(DispatchResult result, CancellationToken cancellationToken);
}
