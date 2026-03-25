namespace InterBus.Middleware;

/// <summary>
/// Carries the state of a single message dispatch through the
/// <see cref="IConsumeMiddleware"/> pipeline and is the <see cref="IConsumeContext"/>
/// passed directly to <see cref="IConsumer{T}.Consume"/>.
/// Middleware may inspect or mutate <see cref="Payload"/> and <see cref="Headers"/>
/// (e.g. to implement the Claim Check pattern) before the terminal step invokes
/// the consumer's dispatch delegate.
/// </summary>
public sealed class ConsumeContext
{
    /// <summary>The descriptor for the consumer that will handle this message.</summary>
    public ConsumerDescriptor Descriptor { get; }

    /// <summary>The serialized message payload. Middleware may replace this value.</summary>
    public byte[] Payload { get; set; }

    /// <summary>
    /// The headers attached to the incoming message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    internal ConsumeContext(
        ConsumerDescriptor descriptor,
        byte[] payload,
        MessageHeaders headers)
    {
        Descriptor = descriptor;
        Payload = payload;
        Headers = headers;
    }
}
