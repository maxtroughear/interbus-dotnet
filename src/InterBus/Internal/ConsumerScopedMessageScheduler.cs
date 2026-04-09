using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Consumer-scoped <see cref="IMessageScheduler"/> implementation.
/// <para>
/// When <see cref="AddPublishScheduling"/> is active (<paramref name="isPublishBased"/> is
/// <c>true</c>), scheduling requests are forwarded as a <see cref="ScheduleDeliveryCommand"/>
/// via the consumer-scoped <see cref="IMessagePublisher"/>. This ensures that consumer-keyed
/// <see cref="ISendMiddleware"/> and <see cref="IPublishMiddleware"/> steps run when a consumer
/// schedules a message from within its <c>Consume</c> method, mirroring the behaviour of
/// <see cref="ConsumerScopedMessagePublisher"/> for immediate sends.
/// </para>
/// <para>
/// When direct scheduling is active (<paramref name="isPublishBased"/> is <c>false</c>),
/// scheduling requests are forwarded directly to the registered
/// <see cref="IMessageSchedulingProvider"/>. Consumer-keyed <see cref="IScheduleMiddleware"/>
/// steps are applied in both cases before the terminal delivery step.
/// </para>
/// </summary>
/// <remarks>
/// Created by <see cref="IConsumerSchedulerFactory"/> implementations and not intended for
/// direct construction by application code.
/// </remarks>
public sealed class ConsumerScopedMessageScheduler(
    InterBusOptions options,
    IMiddlewareFactory middlewareFactory,
    IMessageSerializer serializer,
    Type consumerType,
    IMessagePublisher consumerPublisher,
    bool isPublishBased,
    IMessageSchedulingProvider? provider)
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
        Func<ScheduleContext, CancellationToken, Task> terminal = isPublishBased
            ? BuildPublishTerminal(scheduledAt)
            : BuildDirectTerminal(scheduledAt);

        var middlewares = middlewareFactory.Resolve<IScheduleMiddleware>(consumerType);
        var pipeline = terminal;
        foreach (var middleware in middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline(scheduleContext, cancellationToken);
    }

    private Func<ScheduleContext, CancellationToken, Task> BuildPublishTerminal(DateTimeOffset scheduledAt) =>
        (ctx, ct) =>
        {
            var command = new ScheduleDeliveryCommand
            {
                Destination = ctx.Destination.ToString(),
                Payload = ctx.Payload,
                Headers = new Dictionary<string, string>(ctx.Headers),
                ScheduledAt = scheduledAt
            };
            return consumerPublisher.Send<SchedulingConsumer, ScheduleDeliveryCommand>(command, ct);
        };

    private Func<ScheduleContext, CancellationToken, Task> BuildDirectTerminal(DateTimeOffset scheduledAt) =>
        (ctx, ct) =>
        {
            if (provider == null)
                throw new InvalidOperationException(
                    "No IMessageSchedulingProvider is registered. " +
                    "Call AddTransportDelayScheduler() or register another provider.");
            return provider.ScheduleAsync(ctx.Destination, ctx.Payload, ctx.Headers, scheduledAt, ct);
        };

    private static MessageHeaders Stamp<T>() where T : class =>
        new() { [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name };
}
