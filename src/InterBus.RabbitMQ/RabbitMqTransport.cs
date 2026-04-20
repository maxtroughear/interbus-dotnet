namespace InterBus.RabbitMQ;

/// <summary>
/// Backwards-compatible RabbitMQ transport scheme constants.
/// </summary>
public static class RabbitMqTransport
{
    /// <summary>URI scheme for RabbitMQ topic (fan-out) destinations.</summary>
    public const string TopicScheme = RabbitMqTopicTransport.TopicScheme;

    /// <summary>URI scheme for RabbitMQ queue (point-to-point) destinations.</summary>
    public const string QueueScheme = RabbitMqQueueTransport.QueueScheme;
}
