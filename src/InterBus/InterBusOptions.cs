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

    /// <summary>All registered consumer descriptors.</summary>
    public IReadOnlyList<ConsumerDescriptor> Consumers => _consumers;

    /// <summary>
    /// The <see cref="System.Text.Json.JsonSerializerOptions"/> used to serialize and deserialize
    /// message payloads. To enable trimming and AOT compatibility, configure this with a
    /// source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>.
    /// Defaults to a reflection-based resolver; override for trimmed or AOT applications.
    /// </summary>
#pragma warning disable IL2026 // DefaultJsonTypeInfoResolver uses reflection. Override SerializerOptions with a source-generated JsonSerializerContext for trimmed or AOT applications.
    public JsonSerializerOptions SerializerOptions { get; set; } =
        new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
#pragma warning restore IL2026

    internal void AddConsumer(ConsumerDescriptor descriptor) => _consumers.Add(descriptor);
}
