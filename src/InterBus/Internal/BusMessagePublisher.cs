using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Bus scoped <see cref="IMessagePublisher"/> implementation. Serializes each message and routes it
/// through the configured <see cref="IPublishMiddleware"/> pipeline. The terminal pipeline step
/// resolves the correct <see cref="IInterBusTransport"/> by the destination URI scheme via
/// <see cref="IInterBusTransportFactory"/> and forwards the message.
/// Always stamps the <see cref="InterBusHeaders.MessageType"/> header so consumer dispatch
/// delegates can identify the message type without runtime reflection.
/// </summary>
internal sealed class BusMessagePublisher(InterBusOptions options, IMiddlewareFactory middlewareFactory)
    : IMessagePublisher
{
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
        var pipeline = BuildPublishPipeline<T>();

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
        var pipeline = BuildPublishPipeline<T>();

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
        var pipeline = BuildPublishPipeline<T>();

        return pipeline(context, cancellationToken);
    }

    /// <summary>
    /// Returns a new <see cref="MessageHeaders"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully qualified CLR type name of <typeparamref name="T"/>.
    /// </summary>
    private static MessageHeaders Stamp<T>() where T : class =>
        new() { [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name };

    // Maybe move to base class?
    private Func<PublishContext<TMessage>, CancellationToken, Task> BuildPublishPipeline<TMessage>()
        where TMessage : class
    {
        Func<PublishContext<TMessage>, CancellationToken, Task> pipeline = (_, _) => Task.CompletedTask;

        var middlewares = middlewareFactory.Resolve<IPublishMiddleware>();
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline;
    }
}
