using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using InterBus.Transporting;
using System.Text;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SNS backed <see cref="ITransport"/> for topic (fan-out) delivery.
/// <para>
/// When <see cref="SendAsync"/> is called with a <c>snstopic://</c> destination URI it
/// publishes the message to an SNS topic using
/// <see cref="SnsSqsTransportOptions.TopicPrefix"/><c>{topicName}</c> as the topic name.
/// SNS fans the message out to every SQS queue subscribed to that topic, enabling the
/// publish/subscribe pattern.
/// </para>
/// <para>
/// By default the message body is a JSON <see cref="SnsSqsMessageEnvelope"/> containing the
/// full set of InterBus headers and the base-64 encoded payload, and a small subset of core
/// headers (<c>interbus-message-type</c>, <c>interbus-message-id</c>,
/// <c>interbus-correlation-id</c>) are promoted to SNS message attributes.
/// When <see cref="SnsSqsTransportOptions.UseSnsSerialisedMessageOnSqsByDefault"/> is enabled,
/// the body is emitted as plain UTF-8 payload text and all headers are published as SNS
/// message attributes.
/// </para>
/// <para>
/// Topic ARNs are resolved once and cached for the application lifetime so that create calls
/// are not repeated on every send. ARN resolution and topology mode enforcement are delegated
/// to <see cref="ITopologyBuilder"/>.
/// </para>
/// <para>
/// Delayed delivery is not supported for SNS topic publishes; any
/// <see cref="TransportSendContext.Delay"/> value is ignored.
/// </para>
/// </summary>
public sealed class SnsTransport(
    IAmazonSimpleNotificationService snsClient,
    ITopologyBuilder topologySetup,
    SnsSqsTransportOptions options)
    : ITransport
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
                $"Expected '{TopicScheme}'",
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

        var useSnsSerialisedMessageOnSqs = options.UseSnsSerialisedMessageOnSqsByDefault;
        var body = useSnsSerialisedMessageOnSqs
            ? Encoding.UTF8.GetString(context.Payload)
            : SnsSqsMessageEnvelope.Serialize(context.Payload, context.Headers);
        var messageAttributes = BuildSnsMessageAttributes(
            context.Headers,
            includeAllHeaders: useSnsSerialisedMessageOnSqs);

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
        BuildSnsMessageAttributes(MessageHeaders headers, bool includeAllHeaders)
    {
        var result =
            new Dictionary<string, MessageAttributeValue>(
                includeAllHeaders ? headers.Count : CoreAttributeHeaders.Count,
                StringComparer.Ordinal);

        foreach (var kv in headers)
        {
            if (!includeAllHeaders && !CoreAttributeHeaders.Contains(kv.Key))
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
