namespace InterBus.Internal;

internal sealed class ConsumeContext : IConsumeContext
{
    public IReadOnlyDictionary<string, string> Headers { get; }
    public string? CorrelationId { get; }

    public ConsumeContext(MessageHeaders headers)
    {
        Headers = headers;
        CorrelationId = headers.TryGetValue(InterBusHeaders.CorrelationId, out var value) ? value : null;
    }
}
