using InterBus.Consumer;

namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single message dispatch through the
/// <see cref="IConsumeMiddleware"/> pipeline and is the <see cref="IConsumeContext"/>
/// passed directly to <see cref="IConsumer{T}.Consume"/>.
/// Middleware may inspect or mutate <see cref="Message"/> and <see cref="Headers"/>
/// (e.g. to implement the Claim Check pattern) before the terminal step invokes
/// the consumer's dispatch delegate.
/// </summary>
public sealed class ConsumeContext<T>(ConsumerDescriptor descriptor, T message, MessageHeaders headers) : IContext
{
    /// <summary>The descriptor for the consumer that will handle this message.</summary>
    public ConsumerDescriptor Descriptor { get; } = descriptor;

    /// <summary>The message. Middleware may replace this value.</summary>
    public T Message { get; set; } = message;

    /// <summary>
    /// The headers attached to the incoming message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; } = headers;
}
