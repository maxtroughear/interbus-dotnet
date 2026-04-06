namespace InterBus.Internal;

internal sealed class ConsumeContext(
    MessageHeaders headers,
    IMessagePublisher publisher,
    IMessageScheduler? scheduler) : IConsumeContext
{
    public IReadOnlyDictionary<string, string> Headers { get; } = headers;
    public string? CorrelationId { get; } = headers.GetValueOrDefault(InterBusHeaders.CorrelationId);
    public string MessageType { get; } = headers[InterBusHeaders.MessageType];
    public IMessagePublisher Publisher { get; } = publisher;
    public IMessageScheduler? Scheduler { get; } = scheduler;
}
