using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Abstract base for consumer-scoped <see cref="IMessageScheduler"/> implementations.
/// Handles destination resolution, header stamping (including causation-tracking headers
/// propagated from the triggering consume context), middleware pipeline execution, and
/// serialization. Subclasses provide the terminal delivery step by implementing
/// <see cref="BuildTerminal"/>.
/// </summary>
internal abstract class ConsumerScopedMessageSchedulerBase(
    InterBusOptions options,
    IMiddlewareFactory middlewareFactory,
    IMessageSerializer serializer,
    Type consumerType,
    MessageHeaders consumeHeaders)
    : IMessageScheduler
{
    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublish(message, scheduledAt, _ => { }, cancellationToken);

    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        return ScheduleInternal(message, destination, scheduledAt, configure, cancellationToken);
    }

    public Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSend<TConsumer, T>(message, scheduledAt, _ => { }, cancellationToken);

    public Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer));
        return ScheduleInternal(message, destination, scheduledAt, configure, cancellationToken);
    }

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, scheduledAt, _ => { }, cancellationToken);

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);
        return ScheduleInternal(message, destination, scheduledAt, configure, cancellationToken);
    }

    private Task ScheduleInternal<T>(T message, Uri destination, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure, CancellationToken cancellationToken) where T : class
    {
        var publishContext = new PublishContext<T>(destination, message, Stamp<T>());
        configure(publishContext);
        var payload = serializer.Serialize(publishContext.Message);

        var delay = scheduledAt - DateTimeOffset.UtcNow;
        if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;

        var scheduleContext = new ScheduleContext(destination, payload, publishContext.Headers, delay);

        return ExecuteSchedulePipelineAsync(scheduleContext, scheduledAt, cancellationToken);
    }

    private Task ExecuteSchedulePipelineAsync(ScheduleContext scheduleContext, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
    {
        var pipeline = BuildTerminal(scheduledAt);
        var middlewares = middlewareFactory.Resolve<IScheduleMiddleware>(consumerType);
        foreach (var middleware in middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline(scheduleContext, cancellationToken);
    }

    /// <summary>
    /// Stamps the initial headers for a newly scheduled message.
    /// Propagates the <see cref="InterBusHeaders.CorrelationId"/> from the triggering message
    /// and sets <see cref="InterBusHeaders.CausationId"/> to the triggering message's
    /// <see cref="InterBusHeaders.MessageId"/> to enable causal tracing across deliveries.
    /// </summary>
    private MessageHeaders Stamp<T>() where T : class
    {
        var headers = new MessageHeaders { [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name };

        if (consumeHeaders.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId))
            headers[InterBusHeaders.CorrelationId] = correlationId;

        if (consumeHeaders.TryGetValue(InterBusHeaders.MessageId, out var messageId))
            headers[InterBusHeaders.CausationId] = messageId;

        return headers;
    }

    /// <summary>
    /// Builds the terminal step of the schedule pipeline for the given
    /// <paramref name="scheduledAt"/> delivery time.
    /// </summary>
    protected abstract Func<ScheduleContext, CancellationToken, Task> BuildTerminal(DateTimeOffset scheduledAt);
}
