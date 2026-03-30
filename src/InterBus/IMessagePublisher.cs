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

    /// <summary>
    /// Send a message directly to the named <paramref name="queueName"/>, bypassing topic fan-out.
    /// Use this overload when the consumer type is not available at the call site (e.g. cross-project sends).
    /// </summary>
    Task Send<T>(string queueName, T message, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Send a message directly to the named <paramref name="queueName"/>, attaching the supplied
    /// <paramref name="headers"/>. Use this overload when the consumer type is not available at the call site.
    /// </summary>
    Task Send<T>(string queueName, T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where T : class;

    // -------------------------------------------------------------------------
    // Scheduling: publish to topic at a future point in time
    // -------------------------------------------------------------------------

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>. Requires an <see cref="IMessageScheduler"/>
    /// to be registered (e.g. via the <c>InterBus.TickerQ</c> package).
    /// </summary>
    Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>, attaching the supplied
    /// <paramref name="headers"/>. Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task SchedulePublish<T>(T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> after the
    /// specified <paramref name="delay"/>. Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task SchedulePublish<T>(T message, TimeSpan delay, CancellationToken cancellationToken = default)
        where T : class =>
        SchedulePublish(message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> after the
    /// specified <paramref name="delay"/>, attaching the supplied <paramref name="headers"/>.
    /// Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task SchedulePublish<T>(T message, MessageHeaders headers, TimeSpan delay,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublish(message, headers, DateTimeOffset.UtcNow + delay, cancellationToken);

    // -------------------------------------------------------------------------
    // Scheduling: send directly to a consumer's queue at a future point in time
    // -------------------------------------------------------------------------

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/> at
    /// the absolute UTC time <paramref name="scheduledAt"/>. Requires an <see cref="IMessageScheduler"/>
    /// to be registered.
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/> at
    /// the absolute UTC time <paramref name="scheduledAt"/>, attaching the supplied
    /// <paramref name="headers"/>. Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/> after
    /// the specified <paramref name="delay"/>. Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, TimeSpan delay, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSend<TConsumer, T>(message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/> after
    /// the specified <paramref name="delay"/>, attaching the supplied <paramref name="headers"/>.
    /// Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, MessageHeaders headers, TimeSpan delay,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSend<TConsumer, T>(message, headers, DateTimeOffset.UtcNow + delay, cancellationToken);

    // -------------------------------------------------------------------------
    // Scheduling: send directly to a named queue at a future point in time
    // -------------------------------------------------------------------------

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>. Requires an <see cref="IMessageScheduler"/>
    /// to be registered.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>, attaching the supplied
    /// <paramref name="headers"/>. Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> after the
    /// specified <paramref name="delay"/>. Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, TimeSpan delay,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> after the
    /// specified <paramref name="delay"/>, attaching the supplied <paramref name="headers"/>.
    /// Requires an <see cref="IMessageScheduler"/> to be registered.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, MessageHeaders headers, TimeSpan delay,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, headers, DateTimeOffset.UtcNow + delay, cancellationToken);
}
