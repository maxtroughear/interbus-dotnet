using System.Text.Json;
using Amazon.SimpleNotificationService.Model;

namespace InterBus.AmazonSqs;

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

    public string SerializeForSnsPublish(byte[] payload, MessageHeaders headers)
    {
        var envelope = new InterBusEnvelopeBody(
            Convert.ToBase64String(payload),
            new Dictionary<string, string>(headers, StringComparer.Ordinal));

        return JsonSerializer.Serialize(envelope, SnsSqsJsonContext.Default.InterBusEnvelopeBody);
    }

    public (byte[] Payload, MessageHeaders Headers) DeserializeFromSqs(string messageBody)
    {
        var envelope = JsonSerializer.Deserialize(
                           messageBody,
                           SnsSqsJsonContext.Default.InterBusEnvelopeBody)
                       ?? throw new InvalidOperationException("Failed to deserialize SNS/SQS message envelope");

        var payload = Convert.FromBase64String(envelope.Payload);
        var headers = new MessageHeaders(
            new Dictionary<string, string>(envelope.Headers, StringComparer.Ordinal));

        return (payload, headers);
    }

    public Dictionary<string, MessageAttributeValue> BuildSnsMessageAttributes(MessageHeaders headers)
    {
        var result = new Dictionary<string, MessageAttributeValue>(
            CoreAttributeHeaders.Count,
            StringComparer.Ordinal);

        foreach (var (headerName, headerValue) in headers)
        {
            if (!CoreAttributeHeaders.Contains(headerName))
                continue;

            result[headerName] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = headerValue,
            };
        }

        return result;
    }
}
