namespace InterBus;

/// <summary>
/// Message Endpoint (publisher side). Injects into application services to publish messages into the bus.
/// Registered as scoped by <c>AddInterBus</c> to participate in the caller's unit-of-work.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publish a message to the topic for <typeparamref name="T"/>. All registered consumers for this
    /// message type will receive the message (Publish-Subscribe Channel / fan-out).
    /// </summary>
    Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Publish a message to the topic for <typeparamref name="T"/>, attaching the supplied
    /// <paramref name="headers"/>. All registered consumers will receive the message and headers.
    /// </summary>
    Task Publish<T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message directly to the queue of <typeparamref name="TConsumer"/>, bypassing topic fan-out.
    /// Only <typeparamref name="TConsumer"/> will receive the message (Point-to-Point Channel).
    /// </summary>
    Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Send a message directly to the queue of <typeparamref name="TConsumer"/>, attaching the supplied
    /// <paramref name="headers"/>. Only <typeparamref name="TConsumer"/> will receive the message.
    /// </summary>
    Task Send<TConsumer, T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;
}
