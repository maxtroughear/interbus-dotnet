using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Transporting;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SQS backed <see cref="ITransport"/> for point-to-point queue delivery.
/// </summary>
public sealed class SqsTransport(
    IAmazonSQS sqsClient,
    SnsSqsTopology topology,
    SnsSqsMessageEnvelope rawEnvelope) : ITransport
{
    /// <summary>URI scheme for SQS queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "sqsqueue";

    // Core headers promoted to SQS message attributes for observability and filter policy support.
    private static readonly HashSet<string> CoreAttributeHeaders = new(StringComparer.Ordinal)
    {
        InterBusHeaders.MessageType,
        InterBusHeaders.MessageId,
        InterBusHeaders.CorrelationId,
    };

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        if (context.Destination.Scheme != QueueScheme)
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{QueueScheme}'",
                nameof(context));

        var rawQueueName = RoutingConventions.GetDestinationName(context.Destination);
        await SendToQueueAsync(rawQueueName, context, cancellationToken);
    }

    private async Task SendToQueueAsync(
        string rawQueueName,
        TransportSendContext context,
        CancellationToken ct)
    {
        // Queue must be pre-declared by the consumer's topology builder at startup.
        // URL lookup fails with a clear error if the queue is not configured.
        var queueName = topology.BuildSqsQueueName(rawQueueName);

        if (!topology.QueueUrls.TryGetValue(queueName, out var queueUrl))
            throw new InvalidOperationException(
                $"Queue '{rawQueueName}' has not been declared. Ensure the consumer for this queue " +
                "is registered so that its queue is created during startup topology setup.");

        // Serialise the full payload + headers into the envelope body so that any
        // serialisation format (JSON, MessagePack, gzip, etc.) round-trips without
        // data loss, and header count is never limited by SQS's 10-attribute cap.
        var body = rawEnvelope.SerializeForSnsPublish(context.Payload, context.Headers);

        // Promote a small subset of core headers to SQS message attributes so they are
        // visible without parsing the envelope body.
        var messageAttributes = BuildSqsMessageAttributes(context.Headers);

        // SQS supports delay of 0-900 seconds; clamp accordingly.
        var delaySeconds = context.Delay.HasValue
            ? (int)Math.Clamp(context.Delay.Value.TotalSeconds, 0, 900)
            : 0;

        await sqsClient.SendMessageAsync(
            new SendMessageRequest
            {
                QueueUrl = queueUrl,
                MessageBody = body,
                MessageAttributes = messageAttributes,
                DelaySeconds = delaySeconds,
            },
            ct);
    }

    private static Dictionary<string, MessageAttributeValue> BuildSqsMessageAttributes(
        MessageHeaders headers)
    {
        var result = new Dictionary<string, MessageAttributeValue>(
            CoreAttributeHeaders.Count,
            StringComparer.Ordinal);

        foreach (var kv in headers)
        {
            if (!CoreAttributeHeaders.Contains(kv.Key))
                continue;

            result[kv.Key] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = kv.Value,
            };
        }

        return result;
    }
}
