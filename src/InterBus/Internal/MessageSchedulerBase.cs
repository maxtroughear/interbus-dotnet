using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Base class for <see cref="IMessageScheduler"/> implementations. Handles destination
/// resolution, header stamping, and <see cref="PublishContext{T}"/> construction.
/// Subclasses provide the specific delivery step by implementing
/// <see cref="ScheduleInternalAsync{T}"/>.
/// </summary>
internal abstract class MessageSchedulerBase(InterBusOptions options, IMessageSerializer serializer)
    : IMessageScheduler
{
    /// <summary>The serializer used to produce the wire payload.</summary>
    protected IMessageSerializer Serializer { get; } = serializer;

    public Task SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublishAsync(message, scheduledAt, _ => { }, cancellationToken);

    public Task SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        return ScheduleInternalAsync(context, scheduledAt, cancellationToken);
    }

    public Task ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSendAsync<TConsumer, T>(message, scheduledAt, _ => { }, cancellationToken);

    public Task ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer));
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        return ScheduleInternalAsync(context, scheduledAt, cancellationToken);
    }

    public Task ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSendAsync(queueName, message, scheduledAt, _ => { }, cancellationToken);

    public Task ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        return ScheduleInternalAsync(context, scheduledAt, cancellationToken);
    }

    /// <summary>
    /// Returns a new <see cref="MessageHeaders"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully qualified CLR type name of <typeparamref name="T"/> and a new
    /// <see cref="InterBusHeaders.MessageId"/> set to a V7 <see cref="Guid"/>.
    /// Subclasses may override to stamp additional headers.
    /// </summary>
    protected virtual MessageHeaders Stamp<T>() where T : class =>
        new()
        {
            [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name,
            [InterBusHeaders.MessageId] = Guid.CreateVersion7().ToString()
        };

    /// <summary>
    /// Performs the delivery-specific scheduling step for the given fully-configured
    /// <paramref name="context"/>.
    /// </summary>
    protected abstract Task ScheduleInternalAsync<T>(
        PublishContext<T> context,
        DateTimeOffset scheduledAt,
        CancellationToken cancellationToken)
        where T : class;
}
