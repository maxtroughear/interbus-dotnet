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
        var queueUrl = await ResolveQueueUrlAsync(rawQueueName, ct);

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

        context.Headers.TryGetValue(InterBusHeaders.MessageGroupId, out var messageGroupId);

        await sqsClient.SendMessageAsync(
            new SendMessageRequest
            {
                QueueUrl = queueUrl,
                MessageBody = body,
                MessageAttributes = messageAttributes,
                DelaySeconds = delaySeconds,
                MessageGroupId = messageGroupId,
            },
            ct);
    }

    private async Task<string> ResolveQueueUrlAsync(string rawQueueName, CancellationToken ct)
    {
        // If this is a direct send to an exact queue name, reuse the resolved URL cache.
        if (topology.QueueUrls.TryGetValue(rawQueueName, out var cachedLiteralQueueUrl))
            return cachedLiteralQueueUrl;

        // For InterBus-declared queues, the destination is convention-based and must be normalized.
        var declaredQueueName = topology.BuildSqsQueueName(rawQueueName);
        if (topology.QueueUrls.TryGetValue(declaredQueueName, out var declaredQueueUrl))
            return declaredQueueUrl;

        // Otherwise treat the provided name as an exact SQS queue name (including optional .fifo).
        // Do not create queues here (see #165); resolve URL only.
        try
        {
            var literal = await sqsClient.GetQueueUrlAsync(
                new GetQueueUrlRequest { QueueName = rawQueueName },
                ct);
            topology.QueueUrls.TryAdd(rawQueueName, literal.QueueUrl);
            return literal.QueueUrl;
        }
        catch (QueueDoesNotExistException)
        {
            throw new InvalidOperationException(
                $"Queue '{rawQueueName}' was not found in SQS. InterBus does not create queues on send; " +
                "declare/provision the queue first and pass its exact SQS queue name.");
        }
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
