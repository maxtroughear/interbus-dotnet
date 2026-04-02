using System.Diagnostics.CodeAnalysis;

namespace InterBus;

/// <summary>
/// Runtime options populated by <see cref="IInterBusBuilder"/> during startup.
/// Consumed by transport implementations to initialise their topics and queues.
/// </summary>
public sealed class InterBusOptions
{
    private readonly List<ConsumerDescriptor> _consumers = [];
    private readonly Dictionary<Type, string> _topicNames = [];

    /// <summary>All registered consumer descriptors. One descriptor per consumer class.</summary>
    public IReadOnlyList<ConsumerDescriptor> Consumers => _consumers;

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

    internal void SetTopicName(Type messageType, string topicName) => _topicNames[messageType] = topicName;

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
