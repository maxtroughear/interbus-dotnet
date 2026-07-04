namespace InterBus.Scheduling;

/// <summary>
/// Internal command message published to the bus to request that a message be scheduled
/// for deferred delivery via the configured <see cref="IMessageSchedulingProvider"/>.
/// Consumed by <see cref="SchedulingConsumer"/>.
/// The <see cref="Destination"/> URI encodes both the transport type and whether the message
/// should be published to a topic (fan-out) or sent to a queue (point-to-point).
/// </summary>
internal sealed class ScheduleDeliveryCommand
{
    /// <summary>
    /// The destination URI (e.g. <c>memorytopic://interbus/my-topic</c> or
    /// <c>memoryqueue://interbus/my-queue</c>).
    /// </summary>
    public required string Destination { get; init; }

    /// <summary>The serialized original message payload.</summary>
    public required byte[] Payload { get; init; }

    /// <summary>The enriched headers to forward on delivery, including the original message type.</summary>
    public required Dictionary<string, string> Headers { get; init; }

    /// <summary>The UTC time at which the original message should be delivered.</summary>
    public required DateTimeOffset ScheduledAt { get; init; }
}
