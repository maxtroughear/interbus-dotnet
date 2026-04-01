namespace InterBus;

/// <summary>Convention-based naming and URI construction for topics and queues.</summary>
public static class RoutingConventions
{
    /// <summary>The host segment used in all bus destination URIs.</summary>
    public const string BusHost = "interbus";

    /// <summary>Derives the topic name from a message type using its fully qualified name.</summary>
    public static string TopicName(Type messageType) =>
        messageType.FullName ?? messageType.Name;

    /// <summary>Derives the queue name for a consumer. Each consumer class has exactly one queue.</summary>
    public static string QueueName(Type consumerType) =>
        consumerType.FullName ?? consumerType.Name;

    /// <summary>
    /// Builds a topic destination URI: <c>{scheme}://interbus/{encodedTopicName}</c>.
    /// </summary>
    public static Uri TopicUri(string scheme, string topicName) =>
        new($"{scheme}://{BusHost}/{Uri.EscapeDataString(topicName)}");

    /// <summary>
    /// Builds a topic destination URI from a message type using the convention-based name:
    /// <c>{scheme}://interbus/{encodedTypeName}</c>.
    /// </summary>
    public static Uri TopicUri(string scheme, Type messageType) =>
        TopicUri(scheme, TopicName(messageType));

    /// <summary>
    /// Builds a queue destination URI: <c>{scheme}://interbus/{encodedQueueName}</c>.
    /// </summary>
    public static Uri QueueUri(string scheme, string queueName) =>
        new($"{scheme}://{BusHost}/{Uri.EscapeDataString(queueName)}");

    /// <summary>
    /// Builds a queue destination URI from a consumer type using the convention-based name:
    /// <c>{scheme}://interbus/{encodedTypeName}</c>.
    /// </summary>
    public static Uri QueueUri(string scheme, Type consumerType) =>
        QueueUri(scheme, QueueName(consumerType));

    /// <summary>
    /// Extracts the destination name (topic name or queue name) from a bus destination URI
    /// by URL-decoding the path segment.
    /// </summary>
    public static string GetDestinationName(Uri uri) =>
        Uri.UnescapeDataString(uri.AbsolutePath.TrimStart('/'));
}
