namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single message dispatch through the
/// <see cref="IConsumeMiddleware"/> pipeline and is the <see cref="IConsumeContext"/>
/// passed directly to <see cref="IConsumer{T}.Consume"/>.
/// Middleware may inspect or mutate <see cref="Payload"/> and <see cref="Headers"/>
/// (e.g. to implement the Claim Check pattern) before the terminal step invokes
/// the consumer's dispatch delegate.
/// </summary>
public sealed class ConsumeContext<T>
{
    /// <summary>The descriptor for the consumer that will handle this message.</summary>
    public ConsumerDescriptor Descriptor { get; }

    /// <summary>The serialized message payload. Middleware may replace this value.</summary>
    public T Payload { get; set; }

    /// <summary>
    /// The headers attached to the incoming message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    // TODO: Possible make internal?
    public ConsumeContext(ConsumerDescriptor descriptor, T payload, MessageHeaders headers)
    {
        Descriptor = descriptor;
        Payload = payload;
        Headers = headers;
    }
}