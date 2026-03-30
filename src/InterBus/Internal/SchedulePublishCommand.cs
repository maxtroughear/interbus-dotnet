namespace InterBus.Internal;

/// <summary>
/// Internal command message published to the bus to request that a topic-publish message be
/// scheduled via the configured <see cref="IMessageSchedulingProvider"/>.
/// Consumed by <see cref="SchedulingConsumer"/>.
/// </summary>
internal sealed class SchedulePublishCommand
{
    /// <summary>The topic name to publish to (fan-out to all subscriber queues).</summary>
    public string TopicName { get; init; } = string.Empty;

    /// <summary>The serialized original message payload.</summary>
    public byte[] Payload { get; init; } = [];

    /// <summary>The enriched headers to forward on delivery, including the original message type.</summary>
    public Dictionary<string, string> Headers { get; init; } = [];

    /// <summary>The UTC time at which the original message should be delivered.</summary>
    public DateTimeOffset ScheduledAt { get; init; }
}
