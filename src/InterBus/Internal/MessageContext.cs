namespace InterBus.Internal;

internal sealed class MessageContext : IMessageContext
{
    public IReadOnlyDictionary<string, string> Headers { get; }
    public string? CorrelationId { get; }

    public MessageContext(MessageHeaders headers)
    {
        Headers = headers;
        CorrelationId = headers.TryGetValue(InterBusHeaders.CorrelationId, out var value) ? value : null;
    }
}
