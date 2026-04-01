using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessagePublisher"/> implementation. Serializes each message and routes it
/// through the configured <see cref="IPublishMiddleware"/> pipeline. The terminal pipeline step
/// forwards the message to the registered <see cref="IInterBusTransport"/>.
/// Always stamps the <see cref="InterBusHeaders.MessageType"/> header so consumer dispatch
/// delegates can identify the message type without runtime reflection.
/// </summary>
internal sealed class MessagePublisher(
    IInterBusTransport transport,
    InterBusOptions options,
    IMessageSerializer serializer,
    IEnumerable<IPublishMiddleware> middlewares)
    : IMessagePublisher
{
    private readonly IList<IPublishMiddleware> _middlewares = middlewares.Reverse().ToList(); // Reverse middleware order for the correct execution order

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
            var payload = serializer.Serialize(ctx.Payload);
            return transport.SendAsync(ctx.Destination, payload, ctx.Headers, ct);
        }
    }

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        Publish(message, [], cancellationToken);

    public Task Publish<T>(T message, MessageHeaders headers, CancellationToken cancellationToken = default)
        where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(destination, message, enriched);
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
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer));
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(destination, message, enriched);
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
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);
        var enriched = Enrich<T>(headers);
        var context = new PublishContext<T>(destination, message, enriched);
        var pipeline = BuildMiddlewarePipeline<T>();

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
