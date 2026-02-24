namespace InterBus.Internal;

internal sealed class MessageContext : IMessageContext
{
    public IReadOnlyDictionary<string, string> Headers { get; }

    public MessageContext(MessageHeaders headers)
    {
        Headers = headers;
    }
}
