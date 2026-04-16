using System.Text.Json;
using System.Text.Json.Serialization;

namespace InterBus.AmazonSqs;

/// <summary>
/// Wire-format envelope for SNS/SQS messages. The envelope is serialised as JSON and used
/// as the SQS/SNS message body, carrying both the raw (base-64 encoded) payload and the
/// complete set of InterBus headers.
/// <para>
/// Placing all headers inside the envelope — rather than as SQS/SNS message attributes —
/// removes the 10-attribute limit on SNS and SQS. A small subset of core headers
/// (<see cref="InterBusHeaders.MessageType"/>, <see cref="InterBusHeaders.MessageId"/>,
/// <see cref="InterBusHeaders.CorrelationId"/>) are still copied to message attributes so
/// that SNS subscription filter policies can route messages by type.
/// </para>
/// </summary>
internal sealed record SnsSqsMessageEnvelope(
    [property: JsonPropertyName("p")] string Payload,
    [property: JsonPropertyName("h")] Dictionary<string, string> Headers)
{
    /// <summary>
    /// Serialises <paramref name="payload"/> and <paramref name="headers"/> into a JSON
    /// envelope string suitable for use as an SQS or SNS message body.
    /// </summary>
    internal static string Serialize(byte[] payload, MessageHeaders headers)
    {
        var envelope = new SnsSqsMessageEnvelope(
            Convert.ToBase64String(payload),
            new Dictionary<string, string>(headers, StringComparer.Ordinal));

        return JsonSerializer.Serialize(envelope, SnsSqsJsonContext.Default.SnsSqsMessageEnvelope);
    }

    /// <summary>
    /// Deserialises a JSON envelope string produced by <see cref="Serialize"/> and returns
    /// the original payload bytes and headers.
    /// </summary>
    internal static (byte[] Payload, MessageHeaders Headers) Deserialize(string json)
    {
        var envelope = JsonSerializer.Deserialize(json, SnsSqsJsonContext.Default.SnsSqsMessageEnvelope)
            ?? throw new InvalidOperationException("Failed to deserialise SNS/SQS message envelope.");

        var payload = Convert.FromBase64String(envelope.Payload);
        var headers = new MessageHeaders(
            new Dictionary<string, string>(envelope.Headers, StringComparer.Ordinal));

        return (payload, headers);
    }
}
