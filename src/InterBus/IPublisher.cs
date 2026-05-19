using InterBus.Publishing;

namespace InterBus;

/// <summary>
/// Message Endpoint (publisher side). Injects into application services to publish messages into the bus.
/// Registered as scoped by <c>AddInterBus</c> to participate in the caller's unit-of-work.
/// </summary>
public interface IPublisher
{
    /// <summary>
    /// Publish a message to the topic for <typeparamref name="T"/>. All registered consumers for this
    /// message type will receive the message (Publish-Subscribe Channel / fan-out).
    /// </summary>
    Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Publish a message to the topic for <typeparamref name="T"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="PublishContext{T}"/> before dispatch.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>) and/or an
    /// optional send delay (via <see cref="PublishContext{T}.Delay"/>).
    /// All registered consumers will receive the message.
    /// </summary>
    Task PublishAsync<T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Send a message directly to the queue of <typeparamref name="TConsumer"/>, bypassing topic fan-out.
    /// Only <typeparamref name="TConsumer"/> will receive the message (Point-to-Point Channel).
    /// </summary>
    Task SendAsync<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsume<T>
        where T : class;

    /// <summary>
    /// Send a message directly to the queue of <typeparamref name="TConsumer"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="PublishContext{T}"/> before dispatch.
    /// Use the delegate to set headers and/or an optional send delay.
    /// Only <typeparamref name="TConsumer"/> will receive the message.
    /// </summary>
    Task SendAsync<TConsumer, T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsume<T>
        where T : class;

    /// <summary>
    /// Send a message directly to the named <paramref name="queueName"/>, bypassing topic fan-out.
    /// Use this overload when the consumer type is not available at the call site (e.g. cross-project sends).
    /// </summary>
    Task SendAsync<T>(string queueName, T message, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message directly to the named <paramref name="queueName"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="PublishContext{T}"/> before dispatch.
    /// Use the delegate to set headers and/or an optional send delay.
    /// Use this overload when the consumer type is not available at the call site.
    /// </summary>
    Task SendAsync<T>(string queueName, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Send a message directly to the <paramref name="queue"/>, bypassing topic fan-out.
    /// Use this overload when the consumer type is not available at the call site (e.g. cross-project sends).
    /// </summary>
    Task SendAsync<T>(Uri queue, T message, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message directly to the <paramref name="queue"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="PublishContext{T}"/> before dispatch.
    /// Use the delegate to set headers and/or an optional send delay.
    /// Use this overload when the consumer type is not available at the call site.
    /// </summary>
    Task SendAsync<T>(Uri queue, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class;
}
