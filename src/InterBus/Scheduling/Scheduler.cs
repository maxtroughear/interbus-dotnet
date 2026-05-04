using InterBus.Consuming;
using InterBus.Publishing;

namespace InterBus.Scheduling;

/// <summary>
/// Base class for <see cref="IScheduler"/> implementations. Handles destination
/// resolution, header stamping, and <see cref="PublishContext{T}"/> construction.
/// Subclasses provide the specific delivery step by implementing
/// <see cref="IMessageScheduler.ScheduleInternalAsync{T}"/>.
/// </summary>
internal class Scheduler(
    InterBusOptions options,
    IMessageScheduler messageScheduler,
    ConsumeContextAccessor contextAccessor)
    : IScheduler
{
    public Task<string> SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublishAsync(message, scheduledAt, _ => { }, cancellationToken);

    public Task<string> SchedulePublishAsync<T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var context = new PublishContext<T>(destination, message, Stamp<T>(scheduledAt));
        configure(context);
        return messageScheduler.ScheduleInternalAsync(context, scheduledAt, cancellationToken);
    }

    public Task<string> ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSendAsync<TConsumer, T>(message, scheduledAt, _ => { }, cancellationToken);

    public Task<string> ScheduleSendAsync<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSendAsync(RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer)), message, scheduledAt,
            configure, cancellationToken);

    public Task<string> ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSendAsync(queueName, message, scheduledAt, _ => { }, cancellationToken);

    public Task<string> ScheduleSendAsync<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSendAsync(RoutingConventions.QueueUri(options.QueueUriScheme, queueName), message, scheduledAt,
            configure, cancellationToken);

    public Task<string> ScheduleSendAsync<T>(Uri queue, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var context = new PublishContext<T>(queue, message, Stamp<T>(scheduledAt));
        configure(context);
        return messageScheduler.ScheduleInternalAsync(context, scheduledAt, cancellationToken);
    }

    public Task CancelAsync(string scheduleId, CancellationToken cancellationToken = default) =>
        messageScheduler.CancelInternalAsync(scheduleId, cancellationToken);

    /// <summary>
    /// Returns a new <see cref="MessageHeaders"/> with <see cref="InterBusHeaders.MessageType"/>
    /// stamped to the fully qualified CLR type name of <typeparamref name="T"/> and a new
    /// <see cref="InterBusHeaders.MessageId"/> set to a V7 <see cref="Guid"/>.
    /// Subclasses may override to stamp additional headers.
    /// </summary>
    private MessageHeaders Stamp<T>(DateTimeOffset scheduledAt) where T : class
    {
        var headers = new MessageHeaders
        {
            [InterBusHeaders.MessageType] = RoutingConventions.TopicName(typeof(T)),
            [InterBusHeaders.MessageId] = Guid.CreateVersion7().ToString(),
            [InterBusHeaders.CorrelationId] = Guid.CreateVersion7().ToString(),
            [InterBusHeaders.SentTime] = DateTimeOffset.UtcNow.ToString("O"),
            [InterBusHeaders.ScheduledDeliveryTime] = scheduledAt.ToString("O")
        };

        var consumeHeaders = contextAccessor.ConsumeContext?.Headers;

        if (consumeHeaders?.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId) == true)
            headers[InterBusHeaders.CorrelationId] = correlationId;

        if (consumeHeaders?.TryGetValue(InterBusHeaders.MessageId, out var messageId) == true)
            headers[InterBusHeaders.CausationId] = messageId;

        return headers;
    }
}
