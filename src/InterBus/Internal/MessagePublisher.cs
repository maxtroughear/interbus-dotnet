using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessagePublisher"/> implementation. Serializes each message and routes it
/// through the configured <see cref="IPublishMiddleware"/> pipeline. The terminal pipeline step
/// resolves the correct <see cref="IInterBusTransport"/> by the destination URI scheme via
/// <see cref="IInterBusTransportFactory"/> and forwards the message.
/// Always stamps the <see cref="InterBusHeaders.MessageType"/> header so consumer dispatch
/// delegates can identify the message type without runtime reflection.
/// </summary>
internal sealed class MessagePublisher(
    SendPipelineExecutor sendPipelineExecutor,
    InterBusOptions options,
    IMessageSerializer serializer,
    IEnumerable<IPublishMiddleware> middlewares)
    : IMessagePublisher
{
    // Reverse middleware order for the correct execution order
    private readonly IList<IPublishMiddleware> _middlewares = middlewares.Reverse().ToList();

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

        // Terminal step: resolve the transport by destination URI scheme and forward.
        Task TerminalPublishDelegate(PublishContext<T> ctx, CancellationToken ct)
        {
            var payload = serializer.Serialize(ctx.Payload);
            var sendContext = new SendContext(ctx.Destination, payload, ctx.Headers, ctx.Delay);
            return sendPipelineExecutor.ExecuteAsync(sendContext, ct);
        }
    }

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        Publish(message, _ => { }, cancellationToken);

    public Task Publish<T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        var pipeline = BuildMiddlewarePipeline<T>();

        return pipeline(context, cancellationToken);
    }

    public Task Send<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        Send<TConsumer, T>(message, _ => { }, cancellationToken);

    public Task Send<TConsumer, T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer));
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        var pipeline = BuildMiddlewarePipeline<T>();

        return pipeline(context, cancellationToken);
    }

    public Task Send<T>(string queueName, T message, CancellationToken cancellationToken = default)
        where T : class =>
        Send(queueName, message, _ => { }, cancellationToken);

    public Task Send<T>(string queueName, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        var pipeline = BuildMiddlewarePipeline<T>();

        return pipeline(context, cancellationToken);
    }

    /// <summary>
    /// Returns a new <see cref="MessageHeaders"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully qualified CLR type name of <typeparamref name="T"/>.
    /// </summary>
    private static MessageHeaders Stamp<T>() where T : class =>
        new() { [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name };
}
