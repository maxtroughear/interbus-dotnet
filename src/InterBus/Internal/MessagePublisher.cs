using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessagePublisher"/> implementation. Serializes each message and routes it
/// through the configured <see cref="IPublishMiddleware"/> pipeline. The terminal pipeline step
/// forwards the message to the registered <see cref="IInterBusTransport"/>.
/// Always stamps the <see cref="InterBusHeaders.MessageType"/> header so consumer dispatch
/// delegates can identify the message type without runtime reflection.
/// </summary>
internal sealed class MessagePublisher : IMessagePublisher
{
    private readonly IInterBusTransport _transport;
    private readonly IMessageScheduler? _scheduler;
    private readonly IList<IPublishMiddleware> _middlewares;
    private readonly InterBusOptions _options;
    private readonly IMessageSerializer _serializer;

    public MessagePublisher(
        IInterBusTransport transport,
        InterBusOptions options,
        IMessageSerializer serializer,
        IEnumerable<IPublishMiddleware> middlewares,
        IEnumerable<IMessageScheduler> schedulers)
    {
        _transport = transport;
        _options = options;
        _serializer = serializer;
        _middlewares = middlewares.Reverse().ToList(); // Reverse middleware order for the correct execution order
        _scheduler = schedulers.FirstOrDefault();
    }

    private Func<PublishContext<T>, CancellationToken, Task> BuildMiddlewarePipeline<T>() where T : class
    {
        // Build pipeline: first registered middleware is outermost.
        // Iterate in reverse so each new step wraps the already-built inner chain.
        var pipeline = TerminalPublishDelegate;
        foreach (var middleware in _middlewares)
        {
            var current = middleware;
            var next = pipeline;
            pipeline = (ctx, ct) => current.InvokeAsync(ctx, next, ct);
        }

        return pipeline;

        // Terminal step: forward serialized payload to the transport.
        Task TerminalPublishDelegate(PublishContext<T> ctx, CancellationToken ct)
        {
            var payload = _serializer.Serialize(ctx.Payload);

            return ctx.IsTopic
                ? _transport.PublishToTopicAsync(ctx.Destination, payload, ctx.Headers, ct)
                : _transport.SendToQueueAsync(ctx.Destination, payload, ctx.Headers, ct);
        }
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

        // Terminal step: forward serialized payload to the scheduler.
        Task TerminalScheduleDelegate(PublishContext<T> ctx, CancellationToken ct)
        {
            if (_scheduler is null)
                throw new InvalidOperationException(
                    "No IMessageScheduler is registered. Register a scheduler implementation such as " +
                    "TickerQMessageScheduler via AddTickerQScheduler().");

            var payload = _serializer.Serialize(ctx.Payload);

            return ctx.IsTopic
                ? _scheduler.SchedulePublishToTopicAsync(ctx.Destination, payload, ctx.Headers, scheduledAt, ct)
                : _scheduler.ScheduleSendToQueueAsync(ctx.Destination, payload, ctx.Headers, scheduledAt, ct);
        }
    }

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        Publish(message, [], cancellationToken);

    public Task Publish<T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where T : class
    {
        var topicName = _options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(topicName, isTopic: true, message, enriched);
        var pipeline = BuildMiddlewarePipeline<T>();

        return pipeline(context, cancellationToken);
    }

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        Send<TConsumer, T>(message, [], cancellationToken);

    public Task Send<TConsumer, T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var queueName = RoutingConventions.QueueName(typeof(TConsumer));
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(queueName, isTopic: false, message, enriched);
        var pipeline = BuildMiddlewarePipeline<T>();

        return pipeline(context, cancellationToken);
    }

    public Task Send<T>(string queueName, T message, CancellationToken cancellationToken = default)
        where T : class =>
        Send(queueName, message, [], cancellationToken);

    public Task Send<T>(string queueName, T message, MessageHeaders headers,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(queueName, isTopic: false, message, enriched);
        var pipeline = BuildMiddlewarePipeline<T>();

        return pipeline(context, cancellationToken);
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

    /// <summary>
    /// Returns a copy of <paramref name="headers"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully qualified CLR type name of <typeparamref name="T"/>.
    /// </summary>
    private static MessageHeaders Enrich<T>(MessageHeaders headers) where T : class
    {
        var enriched = new MessageHeaders(headers)
        {
            [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name
        };
        return enriched;
    }
}
