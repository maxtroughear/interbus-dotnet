using InterBus.Middleware;

namespace InterBus.Internal;

/// <summary>
/// <see cref="IMessageScheduler"/> implementation that asynchronously schedules messages by
/// publishing a concrete command message to the bus. The command is consumed by
/// <see cref="SchedulingConsumer"/>, which enlists the registered
/// <see cref="IMessageSchedulingProvider"/> to perform the actual deferred delivery.
/// <para>
/// Register via <c>bus.AddPublishScheduling()</c>. Also register the consumer that processes
/// the commands via <c>bus.AddSchedulingConsumer()</c> - this may be in the same application
/// or in a dedicated consumer service.
/// </para>
/// </summary>
internal sealed class PublishMessageScheduler(
    IMessagePublisher publisher,
    IMessageSerializer serializer,
    InterBusOptions options)
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
        var payload = serializer.Serialize(context.Payload);
        var command = new ScheduleDeliveryCommand
        {
            Destination = destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(context.Headers),
            ScheduledAt = scheduledAt
        };
        return publisher.Send<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
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
        var payload = serializer.Serialize(context.Payload);
        var command = new ScheduleDeliveryCommand
        {
            Destination = destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(context.Headers),
            ScheduledAt = scheduledAt
        };
        return publisher.Send<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
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
        var payload = serializer.Serialize(context.Payload);
        var command = new ScheduleDeliveryCommand
        {
            Destination = destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(context.Headers),
            ScheduledAt = scheduledAt
        };
        return publisher.Send<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
    }

    private static MessageHeaders Stamp<T>() where T : class =>
        new() { [InterBusHeaders.MessageType] = typeof(T).FullName ?? typeof(T).Name };
}
