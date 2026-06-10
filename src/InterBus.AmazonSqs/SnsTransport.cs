using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using InterBus.Transporting;

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
/// Message body and attributes are produced by the configured <see cref="ISnsSqsMessageEnvelope"/>
/// implementation.
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
    ISnsSqsMessageEnvelope envelope)
    : ITransport
{
    /// <summary>URI scheme for SNS topic (fan-out) destinations.</summary>
    public const string TopicScheme = "snstopic";

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

        var body = envelope.SerializeForSnsPublish(context.Payload, context.Headers);
        var messageAttributes = envelope.BuildSnsMessageAttributes(context.Headers);

        context.Headers.TryGetValue(InterBusHeaders.MessageGroupId, out var messageGroupId);

        await snsClient.PublishAsync(
            new PublishRequest
            {
                TopicArn = topicArn,
                Message = body,
                MessageAttributes = messageAttributes,
                MessageGroupId = messageGroupId,
            },
            ct);
    }
}
