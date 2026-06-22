using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.SimpleNotificationService.Model;

namespace InterBus.AmazonSqs;

/// <summary>
/// SNS-serialised notification format: payload is plain UTF-8 SNS message body and headers
/// come from SNS message attributes.
/// </summary>
public sealed class SnsSerialisedMessageEnvelope : ISnsSqsMessageEnvelope
{
    public string SerializeForSnsPublish(byte[] payload, MessageHeaders headers) =>
        Encoding.UTF8.GetString(payload);

    /// <summary>
    /// Serialises the payload and headers into a JSON string suitable for SQS delivery, following the SNS notification
    /// format. So that consumers can process messages from SNS and SQS interchangeably.
    /// Removes the need for raw delivery and supports any number of attributes.
    /// </summary>
    /// <remarks>May not be compatible with other SQS implementations that expect a full SNS format.</remarks>
    public string SerializeForSqsPublish(byte[] payload, MessageHeaders headers)
    {
        var attributes = new Dictionary<string, SnsNotificationAttributeBody?>(headers.Count, StringComparer.Ordinal);
        foreach (var (headerName, headerValue) in headers)
        {
            attributes[headerName] = new SnsNotificationAttributeBody("String", headerValue);
        }

        var envelope = new SnsNotificationEnvelopeBody(Encoding.UTF8.GetString(payload), attributes);

        return JsonSerializer.Serialize(envelope, SnsSqsJsonContext.Default.SnsNotificationEnvelopeBody);
    }

    public (byte[] Payload, MessageHeaders Headers) DeserializeFromSqs(string messageBody)
    {
        var envelope = JsonSerializer.Deserialize(
                           messageBody,
                           SnsSqsJsonContext.Default.SnsNotificationEnvelopeBody)
                       ?? throw new InvalidOperationException(
                           "Failed to deserialize SNS notification envelope");

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

    public Dictionary<string, MessageAttributeValue> BuildSnsMessageAttributes(MessageHeaders headers)
    {
        var result = new Dictionary<string, MessageAttributeValue>(headers.Count, StringComparer.Ordinal);

        foreach (var (headerName, headerValue) in headers)
        {
            result[headerName] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = headerValue,
            };
        }

        return result;
    }
}

internal sealed record SnsNotificationEnvelopeBody(
    [property: JsonPropertyName("Message")]
    string? Message,
    [property: JsonPropertyName("MessageAttributes")]
    Dictionary<string, SnsNotificationAttributeBody?>? MessageAttributes);

internal sealed record SnsNotificationAttributeBody(
    [property: JsonPropertyName("Type")] string? Type,
    [property: JsonPropertyName("Value")] string? Value);
