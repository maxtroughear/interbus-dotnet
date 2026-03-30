namespace InterBus;

/// <summary>
/// Extends <see cref="IMessagePublisher"/> with deferred-delivery scheduling methods.
/// Requires an <see cref="IMessageSchedulingProvider"/> to be registered in DI
/// (e.g. via the <c>InterBus.TickerQ</c> package).
/// </summary>
public interface ISchedulingMessagePublisher : IMessagePublisher
{
    // -------------------------------------------------------------------------
    // Scheduling: publish to topic at a future point in time
    // -------------------------------------------------------------------------

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>. Fan-out to all registered consumers.
    /// </summary>
    Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>, attaching the supplied
    /// <paramref name="headers"/>.
    /// </summary>
    Task SchedulePublish<T>(T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> after
    /// the specified <paramref name="delay"/>.
    /// </summary>
    Task SchedulePublish<T>(T message, TimeSpan delay, CancellationToken cancellationToken = default)
        where T : class =>
        SchedulePublish(message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> after
    /// the specified <paramref name="delay"/>, attaching the supplied <paramref name="headers"/>.
    /// </summary>
    Task SchedulePublish<T>(T message, MessageHeaders headers, TimeSpan delay,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublish(message, headers, DateTimeOffset.UtcNow + delay, cancellationToken);

    // -------------------------------------------------------------------------
    // Scheduling: send directly to a consumer's queue at a future point in time
    // -------------------------------------------------------------------------

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// at the absolute UTC time <paramref name="scheduledAt"/> (point-to-point).
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// at the absolute UTC time <paramref name="scheduledAt"/>, attaching the supplied
    /// <paramref name="headers"/>.
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// after the specified <paramref name="delay"/>.
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, TimeSpan delay, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSend<TConsumer, T>(message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// after the specified <paramref name="delay"/>, attaching the supplied <paramref name="headers"/>.
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
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> at
    /// the absolute UTC time <paramref name="scheduledAt"/>.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> at
    /// the absolute UTC time <paramref name="scheduledAt"/>, attaching the supplied
    /// <paramref name="headers"/>.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> after
    /// the specified <paramref name="delay"/>.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, TimeSpan delay,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> after
    /// the specified <paramref name="delay"/>, attaching the supplied <paramref name="headers"/>.
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, MessageHeaders headers, TimeSpan delay,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, headers, DateTimeOffset.UtcNow + delay, cancellationToken);
}
