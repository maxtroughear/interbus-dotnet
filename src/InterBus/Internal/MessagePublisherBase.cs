using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Base class for <see cref="IMessagePublisher"/> implementations. Handles destination
/// resolution, header stamping, and pipeline invocation. Subclasses provide the
/// specific publish pipeline by implementing <see cref="BuildPublishPipeline{TMessage}"/>.
/// </summary>
internal abstract class MessagePublisherBase(InterBusOptions options) : IMessagePublisher
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

        return BuildPublishPipeline<T>()(context, cancellationToken);
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

        return BuildPublishPipeline<T>()(context, cancellationToken);
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

        return BuildPublishPipeline<T>()(context, cancellationToken);
    }

    /// <summary>
    /// Returns a new <see cref="MessageHeaders"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully qualified CLR type name of <typeparamref name="T"/> and a
    /// new <see cref="InterBusHeaders.MessageId"/> set to a V7 <see cref="Guid"/>.
    /// Subclasses may override this method to stamp additional headers.
    /// </summary>
    protected virtual MessageHeaders Stamp<T>() where T : class =>
        new()
        {
            [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name,
            [InterBusHeaders.MessageId] = Guid.CreateVersion7().ToString()
        };

    /// <summary>
    /// Builds the publish pipeline to invoke for a message of type <typeparamref name="TMessage"/>.
    /// The returned delegate is the outermost step of the pipeline, which ultimately delivers
    /// the message to the transport.
    /// </summary>
    protected abstract Func<PublishContext<TMessage>, CancellationToken, Task>
        BuildPublishPipeline<TMessage>() where TMessage : class;
}
