using InterBus.Middleware;

namespace InterBus.Consuming;

/// <summary>
/// Carries the state of a single message dispatch through the
/// <see cref="IConsumeMiddleware"/> pipeline and is the <see cref="IConsumeContext"/>
/// passed directly to <see cref="IConsumer{T}.Consume"/>
/// </summary>
public sealed class ConsumeContext<T>(ConsumerDescriptor descriptor, T message, MessageHeaders headers)
    : ConsumeContext(descriptor, headers)
{
    /// <summary>The message. Middleware may replace this value.</summary>
    public T Message { get; set; } = message;
}

public class ConsumeContext(ConsumerDescriptor descriptor, MessageHeaders headers) : IContext, IConsumeContext
{
    /// <summary>The descriptor for the consumer that will handle this message.</summary>
    public ConsumerDescriptor Descriptor { get; } = descriptor;

    /// <summary>
    /// The headers attached to the incoming message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; } = headers;

    public string? CorrelationId => Headers.GetValueOrDefault(InterBusHeaders.CorrelationId);
}
