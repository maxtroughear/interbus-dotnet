namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessageScheduler"/> implementation. Serializes each message and
/// forwards directly to the registered <see cref="IMessageSchedulingProvider"/> for deferred
/// delivery. Any cross-cutting delivery concerns (e.g. the Claim Check pattern) are handled
/// by the <see cref="InterBus.Middleware.IScheduleMiddleware"/> pipeline that runs on the
/// provider side via <see cref="InterBus.Middleware.SchedulePipelineExecutor"/> when the job fires.
/// </summary>
internal sealed class MessageScheduler(
    IMessageSchedulingProvider schedulingProvider,
    InterBusOptions options,
    IMessageSerializer serializer)
    : IMessageScheduler
{
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
        var payload = serializer.Serialize(message);
        var enriched = Enrich<T>(headers);
        return schedulingProvider.ScheduleAsync(destination, payload, enriched, scheduledAt, cancellationToken);
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
        var payload = serializer.Serialize(message);
        var enriched = Enrich<T>(headers);
        return schedulingProvider.ScheduleAsync(destination, payload, enriched, scheduledAt, cancellationToken);
    }

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, [], scheduledAt, cancellationToken);

    public Task ScheduleSend<T>(string queueName, T message, MessageHeaders headers, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);
        var payload = serializer.Serialize(message);
        var enriched = Enrich<T>(headers);
        return schedulingProvider.ScheduleAsync(destination, payload, enriched, scheduledAt, cancellationToken);
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
