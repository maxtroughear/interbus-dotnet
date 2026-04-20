using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using InterBus.Transport;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SNS backed <see cref="IInterBusTransport"/> for topic (fan-out) delivery.
/// <para>
/// When <see cref="SendAsync"/> is called with a <c>snstopic://</c> destination URI it
/// publishes the message to an SNS topic using
/// <see cref="SnsSqsTransportOptions.TopicPrefix"/><c>{topicName}</c> as the topic name.
/// SNS fans the message out to every SQS queue subscribed to that topic, enabling the
/// publish/subscribe pattern.
/// </para>
/// <para>
/// The message body is a JSON <see cref="SnsSqsMessageEnvelope"/> containing the full
/// set of InterBus headers and the base-64 encoded payload. A small subset of core
/// headers (<c>interbus-message-type</c>, <c>interbus-message-id</c>,
/// <c>interbus-correlation-id</c>) are additionally promoted to SNS message attributes so
/// that SNS subscription filter policies can route messages by type without needing to
/// deserialise the body.
/// </para>
/// <para>
/// Topic ARNs are resolved once and cached for the application lifetime so that create calls
/// are not repeated on every send. ARN resolution and topology mode enforcement are delegated
/// to <see cref="IInterBusTopologyBuilder"/>.
/// </para>
/// <para>
/// Delayed delivery is not supported for SNS topic publishes; any
/// <see cref="TransportSendContext.Delay"/> value is ignored.
/// </para>
/// </summary>
public sealed class SnsTransport(
    IAmazonSimpleNotificationService snsClient,
    IInterBusTopologyBuilder topologySetup)
    : IInterBusTransport
{
    /// <summary>URI scheme for SNS topic (fan-out) destinations.</summary>
    public const string TopicScheme = "snstopic";

    // Core headers promoted to SNS message attributes for subscription filter policy support.
    private static readonly HashSet<string> CoreAttributeHeaders = new(StringComparer.Ordinal)
    {
        InterBusHeaders.MessageType,
        InterBusHeaders.MessageId,
        InterBusHeaders.CorrelationId,
    };

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        if (context.Destination.Scheme != TopicScheme)
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{TopicScheme}'.",
                nameof(context));

        var rawTopicName = RoutingConventions.GetDestinationName(context.Destination);
        await PublishToTopicAsync(rawTopicName, context, cancellationToken);
    }

    private async Task PublishToTopicAsync(
        string rawTopicName,
        TransportSendContext context,
        CancellationToken ct)
    {
        // Resolve the topic ARN, creating or verifying the topic as configured by
        // SnsSqsTransportOptions.TopologyMode.
        var topicArn = await topologySetup.EnsureTopicAsync(rawTopicName, ct);

        // Serialise the full payload + headers into the envelope body so that any
        // serialisation format (JSON, MessagePack, gzip, etc.) round-trips without
        // data loss, and header count is never limited by SNS's 10-attribute cap.
        var body = SnsSqsMessageEnvelope.Serialize(context.Payload, context.Headers);

        // Promote a small subset of core headers to SNS message attributes so that
        // SNS subscription filter policies can route by message type without parsing
        // the envelope body.
        var messageAttributes = BuildSnsMessageAttributes(context.Headers);

        await snsClient.PublishAsync(
            new PublishRequest
            {
                TopicArn = topicArn,
                Message = body,
                MessageAttributes = messageAttributes,
            },
            ct);
    }

    private static Dictionary<string, MessageAttributeValue>
        BuildSnsMessageAttributes(MessageHeaders headers)
    {
        var result =
            new Dictionary<string, MessageAttributeValue>(
                CoreAttributeHeaders.Count,
                StringComparer.Ordinal);

        foreach (var kv in headers)
        {
            if (!CoreAttributeHeaders.Contains(kv.Key))
                continue;

            result[kv.Key] =
                new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = kv.Value,
                };
        }

        return result;
    }
}
