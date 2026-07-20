using InterBus.Consuming;

namespace InterBus.MessagePumps;

public interface IMessagePumpFactory
{
    IMessagePump Create(ConsumerDescriptor descriptor);
}
