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
    /// The <see cref="IMessageSerializer"/> used to serialise and deserialise message payloads.
    /// Defaults to <see cref="SystemTextJsonMessageSerializer"/> with a reflection-based resolver.
    /// Override via <see cref="IInterBusBuilder.Options"/> before calling <c>AddConsumers()</c>
    /// to use a different serialisation format or to supply a source-generated
    /// <c>JsonSerializerContext</c> for trimmed or Native AOT applications.
    /// </summary>
    public IMessageSerializer Serializer { get; set; }

    /// <summary>
    /// Initialises a new <see cref="InterBusOptions"/> with a <see cref="SystemTextJsonMessageSerializer"/>
    /// backed by a reflection-based JSON resolver. Override <see cref="Serializer"/> via
    /// <c>bus.Options.Serializer</c> for custom serialisation or for trimmed/AOT applications.
    /// </summary>
    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026",
        Justification =
            "SystemTextJsonMessageSerializer defaults to reflection-based JSON. Override Serializer with a SystemTextJsonMessageSerializer(JsonSerializerOptions) or custom IMessageSerializer for AOT/trim applications.")]
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050",
        Justification =
            "SystemTextJsonMessageSerializer defaults to reflection-based JSON. Override Serializer with a SystemTextJsonMessageSerializer(JsonSerializerOptions) or custom IMessageSerializer for AOT applications.")]
    public InterBusOptions()
    {
        Serializer = new SystemTextJsonMessageSerializer();
    }

    internal void AddConsumer(ConsumerDescriptor descriptor) => _consumers.Add(descriptor);

    internal void SetTopicName(Type messageType, string topicName) => _topicNames[messageType] = topicName;

    public bool TryGetTopicName(Type messageType, [NotNullWhen(true)] out string? topicName) =>
        _topicNames.TryGetValue(messageType, out topicName);
}
