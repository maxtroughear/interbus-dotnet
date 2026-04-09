using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// Scoped <see cref="IMessageScheduler"/> implementation. Serializes each message and
/// forwards directly to the registered <see cref="IMessageSchedulingProvider"/> for deferred
/// delivery. Any cross-cutting delivery concerns (e.g. the Claim Check pattern) are handled
/// by the <see cref="IScheduleMiddleware"/> pipeline that runs on the
/// provider side via <see cref="SchedulePipelineExecutor"/> when the job fires.
/// </summary>
internal sealed class MessageScheduler(
    IMessageSchedulingProvider schedulingProvider,
    InterBusOptions options,
    IMessageSerializer serializer)
    : IMessageScheduler
{
    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        SchedulePublish(message, scheduledAt, _ => { }, cancellationToken);

    public Task SchedulePublish<T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var topicName = options.TryGetTopicName(typeof(T), out var customName)
            ? customName
            : RoutingConventions.TopicName(typeof(T));
        var destination = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        var payload = serializer.Serialize(context.Message);
        return schedulingProvider.ScheduleAsync(destination, payload, context.Headers, scheduledAt, cancellationToken);
    }

    public Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class =>
        ScheduleSend<TConsumer, T>(message, scheduledAt, _ => { }, cancellationToken);

    public Task ScheduleSend<TConsumer, T>(T message, DateTimeOffset scheduledAt, Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default)
        where TConsumer : IConsumer<T>
        where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, typeof(TConsumer));
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        var payload = serializer.Serialize(context.Message);
        return schedulingProvider.ScheduleAsync(destination, payload, context.Headers, scheduledAt, cancellationToken);
    }

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        CancellationToken cancellationToken = default) where T : class =>
        ScheduleSend(queueName, message, scheduledAt, _ => { }, cancellationToken);

    public Task ScheduleSend<T>(string queueName, T message, DateTimeOffset scheduledAt,
        Action<PublishContext<T>> configure,
        CancellationToken cancellationToken = default) where T : class
    {
        var destination = RoutingConventions.QueueUri(options.QueueUriScheme, queueName);
        var context = new PublishContext<T>(destination, message, Stamp<T>());
        configure(context);
        var payload = serializer.Serialize(context.Message);
        return schedulingProvider.ScheduleAsync(destination, payload, context.Headers, scheduledAt, cancellationToken);
    }

    private static MessageHeaders Stamp<T>() where T : class =>
        new() { [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name };
}
