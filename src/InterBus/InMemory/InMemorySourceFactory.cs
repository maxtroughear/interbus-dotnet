using InterBus.Consuming;
using InterBus.MessagePumps;

namespace InterBus.InMemory;

internal sealed class InMemorySourceFactory(InMemoryBroker broker) : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor) => new InMemoryMessagePump(descriptor, broker);
}
