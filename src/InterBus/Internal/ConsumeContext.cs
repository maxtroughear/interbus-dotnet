namespace InterBus.Internal;

internal sealed class ConsumeContext(MessageHeaders headers) : IConsumeContext
{
    public IReadOnlyDictionary<string, string> Headers { get; } = headers;
    public string? CorrelationId { get; } = headers.GetValueOrDefault(InterBusHeaders.CorrelationId);
    public string MessageType { get; } = headers[InterBusHeaders.MessageType];
}
