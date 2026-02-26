using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

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
    /// The <see cref="System.Text.Json.JsonSerializerOptions"/> used to serialize and deserialize
    /// message payloads. To enable trimming and AOT compatibility, configure this with a
    /// source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> via
    /// <see cref="IInterBusBuilder.Options"/> before calling <c>AddGeneratedConsumers()</c>.
    /// Defaults to a reflection-based resolver; override for trimmed or AOT applications.
    /// </summary>
    public JsonSerializerOptions SerializerOptions { get; set; }

    /// <summary>
    /// Initializes a new <see cref="InterBusOptions"/> with a reflection-based JSON resolver as the
    /// default. Override <see cref="SerializerOptions"/> via <c>bus.Options.SerializerOptions</c>
    /// with a source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>
    /// for trimmed or Native AOT applications.
    /// </summary>
    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026",
        Justification =
            "DefaultJsonTypeInfoResolver uses reflection. Override SerializerOptions with a source-generated JsonSerializerContext for AOT/trim applications.")]
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050",
        Justification =
            "DefaultJsonTypeInfoResolver requires dynamic code. Override SerializerOptions with a source-generated JsonSerializerContext for AOT applications.")]
    public InterBusOptions()
    {
        SerializerOptions = new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
    }

    internal void AddConsumer(ConsumerDescriptor descriptor) => _consumers.Add(descriptor);

    internal void SetTopicName(Type messageType, string topicName) => _topicNames[messageType] = topicName;

    internal bool TryGetTopicName(Type messageType, [NotNullWhen(true)] out string? topicName) =>
        _topicNames.TryGetValue(messageType, out topicName);
}
