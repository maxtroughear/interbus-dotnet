using System.Diagnostics.CodeAnalysis;
using InterBus.Consuming;
using InterBus.Publishing;

namespace InterBus;

/// <summary>
/// Runtime options populated by <see cref="InterBusBuilder"/> during startup.
/// Consumed by transport implementations to initialise their topics and queues.
/// </summary>
public sealed class InterBusOptions
{
    private readonly List<ConsumerDescriptor> _consumers = [];
    private readonly List<PublisherDescriptor> _publishers = [];
    private readonly Dictionary<Type, string> _topicNames = [];
    private readonly HashSet<Type> _fifoMessageTypes = [];

    /// <summary>All registered consumer descriptors. One descriptor per consumer class.</summary>
    public IReadOnlyList<ConsumerDescriptor> Consumers => _consumers;

    public IReadOnlyList<PublisherDescriptor> Publishers => _publishers;

    public PublisherDescriptor GetPublisherDescriptor<T>() => GetPublisherDescriptor(typeof(T));

    public PublisherDescriptor GetPublisherDescriptor(Type messageType)
    {
        var publisher = _publishers.FirstOrDefault(x => x.MessageType == messageType);

        if (publisher is not null) return publisher;

        publisher = new PublisherDescriptor(messageType, RoutingConventions.TopicUri(TopicUriScheme, messageType));
        AddPublisher(publisher);

        return publisher;
    }

    /// <summary>
    /// The set of message types declared as FIFO via
    /// <c>builder.Configure&lt;T&gt;(c =&gt; c.Fifo(...))</c>.
    /// Transport implementations use this to create FIFO-capable infrastructure (e.g.
    /// SQS/SNS FIFO queues and topics).
    /// </summary>
    public IReadOnlySet<Type> FifoMessageTypes => _fifoMessageTypes;

    /// <summary>
    /// Marks <paramref name="messageType"/> as a FIFO message type.
    /// Idempotent — calling it multiple times for the same type has no effect.
    /// </summary>
    public void AddFifoMessageType(Type messageType) => _fifoMessageTypes.Add(messageType);

    /// <summary>Returns <c>true</c> if <paramref name="messageType"/> has been declared as FIFO.</summary>
    public bool IsFifoMessageType(Type messageType) => _fifoMessageTypes.Contains(messageType);

    /// <summary>
    /// The URI scheme used when constructing topic destination URIs.
    /// Set by the transport registration extension (e.g. <c>AddInMemoryTransport()</c>).
    /// Defaults to <c>"topic"</c> when no transport has been configured.
    /// </summary>
    public string TopicUriScheme { get; private set; } = "topic";

    /// <summary>
    /// The URI scheme used when constructing queue destination URIs.
    /// Set by the transport registration extension (e.g. <c>AddInMemoryTransport()</c>).
    /// Defaults to <c>"queue"</c> when no transport has been configured.
    /// </summary>
    public string QueueUriScheme { get; private set; } = "queue";

    internal void AddConsumer(ConsumerDescriptor descriptor) => _consumers.Add(descriptor);

    internal void AddPublisher(PublisherDescriptor descriptor) => _publishers.Add(descriptor);

    /// <summary>
    /// Sets the URI schemes used when constructing topic and queue destination URIs.
    /// Called by transport registration extension methods.
    /// </summary>
    public void SetTransportUriSchemes(string topicScheme, string queueScheme)
    {
        TopicUriScheme = topicScheme;
        QueueUriScheme = queueScheme;
    }

    public bool TryGetTopicName(Type messageType, [NotNullWhen(true)] out string? topicName) =>
        _topicNames.TryGetValue(messageType, out topicName);
}
