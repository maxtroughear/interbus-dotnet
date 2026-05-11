using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.SimpleNotificationService.Model;

namespace InterBus.AmazonSqs;

/// <summary>
/// Defines how InterBus serialises SNS publishes and deserialises SQS deliveries.
/// </summary>
public interface ISnsSqsMessageEnvelope
{
    bool UsesSnsSerialisedNotificationOnSqs { get; }
    string SerializeForSnsPublish(byte[] payload, MessageHeaders headers);
    Dictionary<string, MessageAttributeValue> BuildSnsMessageAttributes(MessageHeaders headers);
    (byte[] Payload, MessageHeaders Headers) DeserializeFromSqs(string messageBody);
}

/// <summary>
/// Default InterBus envelope format: payload is base64 in JSON body, headers are in JSON body.
/// </summary>
public sealed class SnsSqsMessageEnvelope : ISnsSqsMessageEnvelope
{
    private static readonly HashSet<string> CoreAttributeHeaders = new(StringComparer.Ordinal)
    {
        InterBusHeaders.MessageType,
        InterBusHeaders.MessageId,
        InterBusHeaders.CorrelationId,
    };

    public bool UsesSnsSerialisedNotificationOnSqs => false;

    public string SerializeForSnsPublish(byte[] payload, MessageHeaders headers)
    {
        var envelope = new InterBusEnvelopeBody(
            Convert.ToBase64String(payload),
            new Dictionary<string, string>(headers, StringComparer.Ordinal));

        return JsonSerializer.Serialize(envelope, SnsSqsJsonContext.Default.InterBusEnvelopeBody);
    }

    public Dictionary<string, MessageAttributeValue> BuildSnsMessageAttributes(MessageHeaders headers)
    {
        var result = new Dictionary<string, MessageAttributeValue>(
            CoreAttributeHeaders.Count,
            StringComparer.Ordinal);

        foreach (var (key, value) in headers)
        {
            if (!CoreAttributeHeaders.Contains(key))
                continue;

            result[key] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = value,
            };
        }

        return result;
    }

    public (byte[] Payload, MessageHeaders Headers) DeserializeFromSqs(string messageBody)
    {
        var envelope = JsonSerializer.Deserialize(
            messageBody,
            SnsSqsJsonContext.Default.InterBusEnvelopeBody)
                       ?? throw new InvalidOperationException("Failed to deserialise SNS/SQS message envelope");

        var payload = Convert.FromBase64String(envelope.Payload);
        var headers = new MessageHeaders(
            new Dictionary<string, string>(envelope.Headers, StringComparer.Ordinal));

        return (payload, headers);
    }
}

/// <summary>
/// SNS-serialised notification format: payload is plain UTF-8 SNS message body and headers
/// come from SNS message attributes.
/// </summary>
public sealed class SnsSerialisedMessageEnvelope : ISnsSqsMessageEnvelope
{
    public bool UsesSnsSerialisedNotificationOnSqs => true;

    public string SerializeForSnsPublish(byte[] payload, MessageHeaders headers) =>
        Encoding.UTF8.GetString(payload);

    public Dictionary<string, MessageAttributeValue> BuildSnsMessageAttributes(MessageHeaders headers)
    {
        var result = new Dictionary<string, MessageAttributeValue>(headers.Count, StringComparer.Ordinal);

        foreach (var (key, value) in headers)
        {
            result[key] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = value,
            };
        }

        return result;
    }

    public (byte[] Payload, MessageHeaders Headers) DeserializeFromSqs(string messageBody)
    {
        var envelope = JsonSerializer.Deserialize(
            messageBody,
            SnsSqsJsonContext.Default.SnsNotificationEnvelopeBody)
                       ?? throw new InvalidOperationException(
                           "Failed to deserialise SNS notification envelope");

        var headers = new MessageHeaders();
        if (envelope.MessageAttributes is not null)
        {
            foreach (var (key, value) in envelope.MessageAttributes)
            {
                if (string.IsNullOrEmpty(key) || value?.Value is null)
                    continue;

                headers[key] = value.Value;
            }
        }

        return (Encoding.UTF8.GetBytes(envelope.Message ?? string.Empty), headers);
    }
}

internal sealed record InterBusEnvelopeBody(
    [property: JsonPropertyName("p")] string Payload,
    [property: JsonPropertyName("h")] Dictionary<string, string> Headers);

internal sealed record SnsNotificationEnvelopeBody(
    [property: JsonPropertyName("Message")] string? Message,
    [property: JsonPropertyName("MessageAttributes")] Dictionary<string, SnsNotificationAttributeBody>? MessageAttributes);

internal sealed record SnsNotificationAttributeBody(
    [property: JsonPropertyName("Type")] string? Type,
    [property: JsonPropertyName("Value")] string? Value);
