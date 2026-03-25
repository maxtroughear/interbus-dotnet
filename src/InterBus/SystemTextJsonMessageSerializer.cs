using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace InterBus;

/// <summary>
/// <see cref="IMessageSerializer"/> implementation backed by <see cref="JsonSerializer"/>.
/// This is the default serialiser used by InterBus.
/// </summary>
/// <remarks>
/// For trimmed or Native AOT applications, use the
/// <see cref="SystemTextJsonMessageSerializer(JsonSerializerOptions)"/> constructor and supply
/// options configured with a source-generated
/// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>.
/// </remarks>
public sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// Initialises a new instance with a reflection-based JSON resolver.
    /// Use <see cref="SystemTextJsonMessageSerializer(JsonSerializerOptions)"/> with a
    /// source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>
    /// for trimmed or Native AOT applications, or call
    /// <c>bus.UseSystemTextJsonSerializer(MyAotJsonContext.Default.Options)</c> inside
    /// <c>AddInterBus</c>.
    /// </summary>
    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026",
        Justification =
            "DefaultJsonTypeInfoResolver uses reflection. Use the overload that accepts JsonSerializerOptions with a source-generated JsonSerializerContext for AOT/trim applications.")]
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050",
        Justification =
            "DefaultJsonTypeInfoResolver requires dynamic code. Use the overload that accepts JsonSerializerOptions with a source-generated JsonSerializerContext for AOT applications.")]
    public SystemTextJsonMessageSerializer()
        : this(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() })
    {
    }

    /// <summary>
    /// Initialises a new instance with the provided <paramref name="options"/>.
    /// Supply options configured with a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> for trimmed or
    /// Native AOT applications.
    /// </summary>
    public SystemTextJsonMessageSerializer(JsonSerializerOptions options) => _options = options;

    /// <inheritdoc />
    public byte[] Serialize<T>(T message) where T : class =>
        JsonSerializer.SerializeToUtf8Bytes(message, (JsonTypeInfo<T>)_options.GetTypeInfo(typeof(T)));

    /// <inheritdoc />
    public T? Deserialize<T>(byte[] payload) where T : class =>
        JsonSerializer.Deserialize(payload, (JsonTypeInfo<T>)_options.GetTypeInfo(typeof(T)));
}
