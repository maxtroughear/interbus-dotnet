namespace InterBus.Internal;

/// <summary>Convention-based naming for topics and queues.</summary>
internal static class RoutingConventions
{
    /// <summary>Derives the topic name from a message type using its fully-qualified name.</summary>
    public static string TopicName(Type messageType) =>
        messageType.FullName ?? messageType.Name;

    /// <summary>Derives the queue name for a specific consumer and message type combination.</summary>
    public static string QueueName(Type consumerType, Type messageType) =>
        $"{consumerType.FullName ?? consumerType.Name}/{messageType.FullName ?? messageType.Name}";
}
