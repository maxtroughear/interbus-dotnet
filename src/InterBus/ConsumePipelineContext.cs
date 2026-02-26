namespace InterBus;

/// <summary>
/// Carries the state of a single message dispatch through the
/// <see cref="IConsumeMiddleware"/> pipeline.
/// Middleware may inspect or mutate <see cref="Payload"/> and <see cref="Headers"/>
/// (e.g. to implement the Claim Check pattern) before the terminal step invokes
/// the consumer's dispatch delegate.
/// </summary>
public sealed class ConsumePipelineContext
{
    /// <summary>The descriptor for the consumer that will handle this message.</summary>
    public ConsumerDescriptor Descriptor { get; }

    /// <summary>The serialized message payload. Middleware may replace this value.</summary>
    public string Payload { get; set; }

    /// <summary>
    /// The headers attached to the incoming message.
    /// Middleware may add, remove, or modify entries.
    /// </summary>
    public MessageHeaders Headers { get; set; }

    /// <summary>
    /// The DI scope for the current message dispatch. Use this to resolve
    /// additional services within middleware (e.g. a claim-check store).
    /// </summary>
    public IServiceProvider ServiceProvider { get; }

    internal ConsumePipelineContext(
        ConsumerDescriptor descriptor,
        string payload,
        MessageHeaders headers,
        IServiceProvider serviceProvider)
    {
        Descriptor = descriptor;
        Payload = payload;
        Headers = headers;
        ServiceProvider = serviceProvider;
    }
}
