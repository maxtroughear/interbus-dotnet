using Amazon.SimpleNotificationService.Model;

namespace InterBus.AmazonSqs;

/// <summary>
/// Defines how InterBus serialises SNS publishes and deserialises SQS deliveries.
/// </summary>
public interface ISnsSqsMessageEnvelope
{
    string SerializeForSnsPublish(byte[] payload, MessageHeaders headers);
    string SerializeForSqsPublish(byte[] payload, MessageHeaders headers);
    (byte[] Payload, MessageHeaders Headers) DeserializeFromSqs(string messageBody);
    Dictionary<string, MessageAttributeValue> BuildSnsMessageAttributes(MessageHeaders headers);
}
