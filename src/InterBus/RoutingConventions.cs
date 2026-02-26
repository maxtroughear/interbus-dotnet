namespace InterBus;

/// <summary>Convention-based naming for topics and queues.</summary>
public static class RoutingConventions
{
    /// <summary>Derives the topic name from a message type using its fully qualified name.</summary>
    public static string TopicName(Type messageType) =>
        messageType.FullName ?? messageType.Name;

    /// <summary>Derives the queue name for a consumer. Each consumer class has exactly one queue.</summary>
    public static string QueueName(Type consumerType) =>
        consumerType.FullName ?? consumerType.Name;
}
