using InterBus.Serialization;

namespace InterBus;

/// <summary>
/// Abstracts message serialisation and deserialisation so that different wire formats
/// can be used without coupling the publisher or consumers to a specific library.
/// </summary>
/// <remarks>
/// The default implementation is <see cref="SystemTextJsonMessageSerializer"/>, which
/// uses <see cref="System.Text.Json.JsonSerializer"/>. Register a custom implementation
/// via <see cref="InterBusBuilder.UseSystemTextJsonSerializer"/> before calling <c>AddConsumers()</c>.
/// </remarks>
public interface IMessageSerializer
{
    /// <summary>Serialises <paramref name="message"/> to a byte array payload</summary>
    byte[] Serialize<T>(T message) where T : class;

    /// <summary>Deserialises <paramref name="payload"/> to an instance of <typeparamref name="T"/></summary>
    T? Deserialize<T>(byte[] payload) where T : class;
}
