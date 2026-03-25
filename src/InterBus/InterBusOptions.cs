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

    internal void AddConsumer(ConsumerDescriptor descriptor) => _consumers.Add(descriptor);

    internal void SetTopicName(Type messageType, string topicName) => _topicNames[messageType] = topicName;

    public bool TryGetTopicName(Type messageType, [NotNullWhen(true)] out string? topicName) =>
        _topicNames.TryGetValue(messageType, out topicName);
}
