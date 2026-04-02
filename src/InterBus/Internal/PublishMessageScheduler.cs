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
        var command = new ScheduleDeliveryCommand
        {
            Destination = destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(enriched),
            ScheduledAt = scheduledAt
        };
        return publisher.Send<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
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
        var command = new ScheduleDeliveryCommand
        {
            Destination = destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(enriched),
            ScheduledAt = scheduledAt
        };
        return publisher.Send<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
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
        var command = new ScheduleDeliveryCommand
        {
            Destination = destination.ToString(),
            Payload = payload,
            Headers = new Dictionary<string, string>(enriched),
            ScheduledAt = scheduledAt
        };
        return publisher.Send<SchedulingConsumer, ScheduleDeliveryCommand>(command, cancellationToken);
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
