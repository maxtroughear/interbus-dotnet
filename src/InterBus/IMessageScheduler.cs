namespace InterBus;

/// <summary>
/// Schedules messages for deferred delivery. Implemented by transports or dedicated scheduler
/// packages (e.g. <c>InterBus.TickerQ</c>) and registered in DI.
/// <para>
/// Users typically interact with scheduling via the <see cref="IMessagePublisher"/> scheduling
/// methods, which resolve this interface to perform the actual scheduling.
/// </para>
/// </summary>
public interface IMessageScheduler
{
    /// <summary>
    /// Schedule a serialised payload to be published to the named topic at
    /// <paramref name="scheduledAt"/> (fan-out to all subscriber queues).
    /// </summary>
    Task SchedulePublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken);

    /// <summary>
    /// Schedule a serialised payload to be sent directly to the named queue at
    /// <paramref name="scheduledAt"/> (bypassing topic fan-out).
    /// </summary>
    Task ScheduleSendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken);
}
