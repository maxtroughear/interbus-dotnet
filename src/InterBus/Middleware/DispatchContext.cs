namespace InterBus.Middleware;

public sealed class DispatchContext(ConsumerDescriptor descriptor, byte[] payload, MessageHeaders headers)
{
    public ConsumerDescriptor Descriptor { get; } = descriptor;

    public byte[] Payload { get; } = payload;

    public MessageHeaders Headers { get; } = headers;
}
