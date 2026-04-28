using InterBus.Consuming;
using InterBus.Middleware;

namespace InterBus.Dispatching;

public sealed class DispatchContext(ConsumerDescriptor descriptor, byte[] payload, MessageHeaders headers) : IContext
{
    public ConsumerDescriptor Descriptor { get; } = descriptor;

    public byte[] Payload { get; } = payload;

    public MessageHeaders Headers { get; } = headers;
}
