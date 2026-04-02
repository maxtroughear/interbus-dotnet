using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessageScheduler"/> implementation. Serializes each message, runs it
/// through the configured <see cref="IPublishMiddleware"/> pipeline, then through the
/// <see cref="IScheduleMiddleware"/> pipeline (with already-serialized bytes), and finally
/// forwards to the registered <see cref="IMessageSchedulingProvider"/> for deferred delivery.
/// </summary>
internal sealed class MessageScheduler(
    IMessageSchedulingProvider schedulingProvider,
    InterBusOptions options,
    IMessageSerializer serializer,
    IEnumerable<IPublishMiddleware> publishMiddlewares,
    IEnumerable<IScheduleMiddleware> scheduleMiddlewares)
    : IMessageScheduler
{
    private readonly IList<IPublishMiddleware> _publishMiddlewares = publishMiddlewares.Reverse().ToList();
    private readonly IList<IScheduleMiddleware> _scheduleMiddlewares = scheduleMiddlewares.Reverse().ToList();

    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublish(message, [], scheduledAt, cancellationToken);

    public Task SchedulePublish<T>(T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(destination, message, enriched);
        var pipeline = BuildPublishPipeline<T>(scheduledAt);

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
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer));
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(destination, message, enriched);
        var pipeline = BuildPublishPipeline<T>(scheduledAt);

        return pipeline(context, cancellationToken);
    }

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, [], scheduledAt, cancellationToken);

    public Task ScheduleSend<T>(string queueName, T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(destination, message, enriched);
        var pipeline = BuildPublishPipeline<T>(scheduledAt);

        return pipeline(context, cancellationToken);
    }

    private Func<PublishContext<T>, CancellationToken, Task> BuildPublishPipeline<T>(
        DateTimeOffset scheduledAt) where T : class
    {
        var schedulePipeline = BuildSchedulePipeline();

        var pipeline = SerializeAndEnterSchedulePipeline;
        foreach (var middleware in _publishMiddlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline;

        // Terminal publish step: serialize and hand off to the schedule middleware pipeline.
        Task SerializeAndEnterSchedulePipeline(PublishContext<T> ctx, CancellationToken ct)
        {
            var payload = serializer.Serialize(ctx.Payload);
            var scheduleContext = new ScheduleContext(ctx.Destination, payload, ctx.Headers, scheduledAt);
            return schedulePipeline(scheduleContext, ct);
        }
    }

    private Func<ScheduleContext, CancellationToken, Task> BuildSchedulePipeline()
    {
        var pipeline = TerminalScheduleDelegate;
        foreach (var middleware in _scheduleMiddlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline;

        // Terminal step: forward serialized payload to the scheduling provider.
        // Use ctx.ScheduledAt so that middleware mutations to the delivery time are honoured.
        Task TerminalScheduleDelegate(ScheduleContext ctx, CancellationToken ct) =>
            schedulingProvider.ScheduleAsync(ctx.Destination, ctx.Payload, ctx.Headers, ctx.ScheduledAt, ct);
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
