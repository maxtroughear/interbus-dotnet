using InterBus.Dispatching;

namespace InterBus.MessagePumps;

public interface IMessage
{
    // TODO: Expose the message body and headers
    // TODO: Transport pumps should unwrap transport specific envelope before handing over here
    // TODO: Allows a core implementation of consumer dispatching.
    // Pumps -> Core buffer -> Core dispatching -> Consumer
    // e.g. 3 pumps to keep buffer (bounded channel) healthy, and X consumers
    public Task Acknowledge(CancellationToken cancellationToken);
    public Task Abandon(CancellationToken cancellationToken);
    public Task DeadLetter(DispatchResult result, CancellationToken cancellationToken);
}
