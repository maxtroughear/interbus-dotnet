using InterBus.Publishing;

namespace InterBus;

/// <summary>
/// Deferred-delivery message scheduler. Injects into application services to schedule messages
/// for future delivery via the configured <see cref="IMessageSchedulingProvider"/>.
/// Requires an <see cref="IMessageSchedulingProvider"/> to be registered in DI
/// (e.g. via the <c>InterBus.TickerQ</c> package).
/// </summary>
public interface IScheduler
{
    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>. Fan-out to all registered consumers.
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt, CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> at the
    /// absolute UTC time <paramref name="scheduledAt"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="PublishContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> after
    /// the specified <paramref name="delay"/>.
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> SchedulePublishAsync<T>(T message, TimeSpan delay, CancellationToken cancellationToken = default)
        where T : class =>
        SchedulePublishAsync(message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be published to the topic for <typeparamref name="T"/> after
    /// the specified <paramref name="delay"/>, applying the supplied <paramref name="configure"/>
    /// delegate to the <see cref="PublishContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> SchedulePublishAsync<T>(T message, TimeSpan delay, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublishAsync(message, DateTimeOffset.UtcNow + delay, configure, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// at the absolute UTC time <paramref name="scheduledAt"/> (point-to-point).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// at the absolute UTC time <paramref name="scheduledAt"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="PublishContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// after the specified <paramref name="delay"/>.
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<TConsumer, T>(T message, TimeSpan delay, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSendAsync<TConsumer, T>(message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the queue of <typeparamref name="TConsumer"/>
    /// after the specified <paramref name="delay"/>, applying the supplied <paramref name="configure"/>
    /// delegate to the <see cref="PublishContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<TConsumer, T>(T message, TimeSpan delay, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSendAsync<TConsumer, T>(message, DateTimeOffset.UtcNow + delay, configure, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> at
    /// the absolute UTC time <paramref name="scheduledAt"/>.
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> at
    /// the absolute UTC time <paramref name="scheduledAt"/>, applying the supplied
    /// <paramref name="configure"/> delegate to the <see cref="PublishContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message of type <typeparamref name="T"/> to be sent to a queue at the specified
    /// absolute UTC time <paramref name="scheduledAt"/>
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<T>(Uri queue, T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> after
    /// the specified <paramref name="delay"/>.
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<T>(string queueName, T message, TimeSpan delay,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSendAsync(queueName, message, DateTimeOffset.UtcNow + delay, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the named <paramref name="queueName"/> after
    /// the specified <paramref name="delay"/>, applying the supplied <paramref name="configure"/>
    /// delegate to the <see cref="PublishContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<T>(string queueName, T message, TimeSpan delay, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSendAsync(queueName, message, DateTimeOffset.UtcNow + delay, configure, cancellationToken);

    /// <summary>
    /// Schedule a message to be sent directly to the <paramref name="queue"/> after
    /// the specified <paramref name="delay"/>, applying the supplied <paramref name="configure"/>
    /// delegate to the <see cref="PublishContext{T}"/> before scheduling.
    /// Use the delegate to set headers (via <see cref="PublishContext{T}.Headers"/>).
    /// </summary>
    /// <returns>An opaque schedule identifier that can be passed to <see cref="CancelAsync"/>.</returns>
    Task<Guid> ScheduleSendAsync<T>(Uri queue, T message, TimeSpan delay, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSendAsync(queue, message, DateTimeOffset.UtcNow + delay, configure, cancellationToken);

    /// <summary>
    /// Cancels a previously scheduled delivery identified by <paramref name="scheduleId"/>.
    /// </summary>
    /// <param name="scheduleId">
    /// The identifier returned by a <c>Schedule*Async</c> overload when the schedule was created.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CancelAsync(Guid scheduleId, CancellationToken cancellationToken = default);
}
