using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Consumer-scoped <see cref="IMessagePublisher"/> implementation. Routes publish and send
/// operations through the consumer-specific <see cref="IPublishMiddleware"/> and
/// <see cref="ISendMiddleware"/> pipelines registered for <paramref name="consumerType"/>.
/// Used by <see cref="ConsumeContext"/> so that middleware registered via
/// <c>UsePublishMiddleware</c> and <c>UseSendMiddleware</c> on a consumer builder is
/// applied when the consumer publishes or sends a message from within its
/// <c>Consume</c> method.
/// </summary>
internal sealed class ConsumerScopedMessagePublisher(
    InterBusOptions options,
    IMiddlewareFactory middlewareFactory,
    IInterBusTransportFactory transportFactory,
    IMessageSerializer serializer,
    Type consumerType) : IMessagePublisher
{
    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        Publish(message, _ => { }, cancellationToken);

    public Task Publish<T>(T message, Action<PublishContext<T>>? configure = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure?.Invoke(context);
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

    private Func<PublishContext<TMessage>, CancellationToken, Task> BuildPublishPipeline<TMessage>()
        where TMessage : class
    {
        // Terminal publish step: serialize and forward through the consumer-scoped send pipeline.
        Func<PublishContext<TMessage>, CancellationToken, Task> pipeline = (ctx, ct) =>
        {
            var payload = serializer.Serialize(ctx.Message);
            var sendContext = new SendContext(ctx.Destination, payload, ctx.Headers, ctx.Delay);
            return ExecuteSendPipelineAsync(sendContext, ct);
        };

        var middlewares = middlewareFactory.Resolve<IPublishMiddleware>(consumerType);
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, ct) => middleware.InvokeAsync(ctx, next, ct);
        }

        return pipeline;
    }

    private Task ExecuteSendPipelineAsync(SendContext context, CancellationToken ct)
    {
        Func<SendContext, CancellationToken, Task> pipeline = (ctx, token) =>
        {
            var transport = transportFactory.Resolve(ctx.Destination);
            var transportContext = new TransportSendContext(ctx.Destination, ctx.Payload, ctx.Headers, ctx.Delay);
            return transport.SendAsync(transportContext, token);
        };

        var middlewares = middlewareFactory.Resolve<ISendMiddleware>(consumerType);
        foreach (var middleware in middlewares)
        {
            var next = pipeline;
            pipeline = (ctx, token) => middleware.InvokeAsync(ctx, next, token);
        }

        return pipeline(context, ct);
    }
}
