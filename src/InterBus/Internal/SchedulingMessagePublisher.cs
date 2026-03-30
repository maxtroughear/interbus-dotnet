using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessageScheduler"/> implementation. Serializes each message, runs it
/// through the configured <see cref="IPublishMiddleware"/> pipeline, and forwards to the
/// registered <see cref="IMessageSchedulingProvider"/> for deferred delivery.
/// </summary>
internal sealed class SchedulingMessagePublisher : IMessageScheduler
{
    private readonly IMessageSchedulingProvider _schedulingProvider;
    private readonly IList<IPublishMiddleware> _middlewares;
    private readonly InterBusOptions _options;
    private readonly IMessageSerializer _serializer;

    public SchedulingMessagePublisher(
        IMessageSchedulingProvider schedulingProvider,
        InterBusOptions options,
        IMessageSerializer serializer,
        IEnumerable<IPublishMiddleware> middlewares)
    {
        _schedulingProvider = schedulingProvider;
        _options = options;
        _serializer = serializer;
        _middlewares = middlewares.Reverse().ToList();
    }

    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublish(message, [], scheduledAt, cancellationToken);

    public Task SchedulePublish<T>(T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class
    {
        var topicName = _options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(topicName, isTopic: true, message, enriched);
        var pipeline = BuildScheduledMiddlewarePipeline<T>(scheduledAt);

        return pipeline(context, cancellationToken);
    }

    public Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSend<TConsumer, T>(message, [], scheduledAt, cancellationToken);

    public Task ScheduleSend<TConsumer, T>(T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var queueName = RoutingConventions.QueueName(typeof(TConsumer));
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(queueName, isTopic: false, message, enriched);
        var pipeline = BuildScheduledMiddlewarePipeline<T>(scheduledAt);

        return pipeline(context, cancellationToken);
    }

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, [], scheduledAt, cancellationToken);

    public Task ScheduleSend<T>(string queueName, T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class
    {
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(queueName, isTopic: false, message, enriched);
        var pipeline = BuildScheduledMiddlewarePipeline<T>(scheduledAt);

        return pipeline(context, cancellationToken);
    }

    private Func<PublishContext<T>, CancellationToken, Task> BuildScheduledMiddlewarePipeline<T>(
        DateTimeOffset scheduledAt) where T : class
    {
        var pipeline = TerminalScheduleDelegate;
        foreach (var middleware in _middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline;

        // Terminal step: serialize and forward to the scheduling provider.
        Task TerminalScheduleDelegate(PublishContext<T> ctx, CancellationToken ct)
        {
            var payload = _serializer.Serialize(ctx.Payload);

            return ctx.IsTopic
                ? _schedulingProvider.SchedulePublishToTopicAsync(
                    ctx.Destination, payload, ctx.Headers, scheduledAt, ct)
                : _schedulingProvider.ScheduleSendToQueueAsync(
                    ctx.Destination, payload, ctx.Headers, scheduledAt, ct);
        }
    }

    private static MessageHeaders Enrich<T>(MessageHeaders headers) where T : class
    {
        var enriched = new MessageHeaders(headers)
        {
            [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name
        };
        return enriched;
    }
}
