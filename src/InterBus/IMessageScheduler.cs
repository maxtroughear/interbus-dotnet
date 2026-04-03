using InterBus.Middleware;

namespace InterBus;

/// <summary>
/// Deferred-delivery message scheduler. Injects into application services to schedule messages
/// for future delivery via the configured <see cref="IMessageSchedulingProvider"/>.
/// Requires an <see cref="IMessageSchedulingProvider"/> to be registered in DI
/// (e.g. via the <c>InterBus.TickerQ</c> package).
/// </summary>
public interface IMessageScheduler
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
    /// absolute UTC time <paramref name="scheduledAt"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="ScheduleContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="ScheduleContext{T}.Headers"/>).
    /// </summary>
    Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt, Action<ScheduleContext<T>> configure,
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
    /// the specified <paramref name="delay"/>, applying the supplied <paramref name="configure"/>
    /// delegate to the <see cref="ScheduleContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="ScheduleContext{T}.Headers"/>).
    /// </summary>
    Task SchedulePublish<T>(T message, TimeSpan delay, Action<ScheduleContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublish(message, DateTimeOffset.UtcNow + delay, configure, cancellationToken);

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
    /// at the absolute UTC time <paramref name="scheduledAt"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="ScheduleContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="ScheduleContext{T}.Headers"/>).
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt, Action<ScheduleContext<T>> configure,
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
    /// after the specified <paramref name="delay"/>, applying the supplied <paramref name="configure"/>
    /// delegate to the <see cref="ScheduleContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="ScheduleContext{T}.Headers"/>).
    /// </summary>
    Task ScheduleSend<TConsumer, T>(T message, TimeSpan delay, Action<ScheduleContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSend<TConsumer, T>(message, DateTimeOffset.UtcNow + delay, configure, cancellationToken);

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
    /// the absolute UTC time <paramref name="scheduledAt"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="ScheduleContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="ScheduleContext{T}.Headers"/>).
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<ScheduleContext<T>> configure,
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
    /// the specified <paramref name="delay"/>, applying the supplied <paramref name="configure"/>
    /// delegate to the <see cref="ScheduleContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="ScheduleContext{T}.Headers"/>).
    /// </summary>
    Task ScheduleSend<T>(string queueName, T message, TimeSpan delay, Action<ScheduleContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, DateTimeOffset.UtcNow + delay, configure, cancellationToken);
}
