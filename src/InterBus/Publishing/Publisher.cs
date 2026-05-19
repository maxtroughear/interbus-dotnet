using InterBus.Consuming;

namespace InterBus.Publishing;

/// <summary>
/// Base class for <see cref="IPublisher"/> implementations. Handles destination
/// resolution, header stamping, and pipeline invocation
/// </summary>
internal class Publisher(
    InterBusOptions options,
    ConsumeContextAccessor contextAccessor,
    PublishPipelineExecutor pipelineExecutor) : IPublisher
{
    public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        PublishAsync(message, _ => { }, cancellationToken);

    public Task PublishAsync<T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);

        return pipelineExecutor.ExecuteAsync(context, cancellationToken);
    }

    public Task SendAsync<TConsumer, T>(T message, CancellationToken cancellationToken = default)
        where TConsumer : IConsume<T>
        where T : class =>
        SendAsync<TConsumer, T>(message, _ => { }, cancellationToken);

    public Task SendAsync<TConsumer, T>(T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsume<T>
        where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer));
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);

        return pipelineExecutor.ExecuteAsync(context, cancellationToken);
    }

    public Task SendAsync<T>(string queueName, T message, CancellationToken cancellationToken = default)
        where T : class =>
        SendAsync(queueName, message, _ => { }, cancellationToken);

    public Task SendAsync<T>(string queueName, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class =>
        SendAsync(RoutingConventions.QueueUri(options.QueueUriScheme, queueName), message, configure,
            cancellationToken);

    public Task SendAsync<T>(Uri queue, T message, CancellationToken cancellationToken = default) where T : class =>
        SendAsync(queue, message, _ => { }, cancellationToken);

    public Task SendAsync<T>(Uri queue, T message, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var context = new PublishContext<T>(queue, message, Stamp<T>());
        configure(context);

        return pipelineExecutor.ExecuteAsync(context, cancellationToken);
    }

    /// <summary>
    /// Returns a new <see cref="MessageHeaders"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully qualified CLR type name of <typeparamref name="T"/> and a
    /// new <see cref="InterBusHeaders.MessageId"/> set to a V7 <see cref="Guid"/>.
    /// Subclasses may override this method to stamp additional headers.
    /// </summary>
    private MessageHeaders Stamp<T>() where T : class
    {
        var headers = new MessageHeaders
        {
            [InterBusHeaders.MessageType] = RoutingConventions.TopicName(typeof(T)),
            [InterBusHeaders.MessageId] = Guid.CreateVersion7().ToString(),
            [InterBusHeaders.CorrelationId] = Guid.CreateVersion7().ToString(),
            [InterBusHeaders.SentTime] = DateTimeOffset.UtcNow.ToString("O"),
        };

        var consumeHeaders = contextAccessor.ConsumeContext?.Headers;

        if (consumeHeaders?.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId) == true)
            headers[InterBusHeaders.CorrelationId] = correlationId;

        if (consumeHeaders?.TryGetValue(InterBusHeaders.MessageId, out var messageId) == true)
            headers[InterBusHeaders.CausationId] = messageId;

        return headers;
    }
}
