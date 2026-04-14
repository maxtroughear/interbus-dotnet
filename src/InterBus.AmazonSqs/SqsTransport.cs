using Amazon.SQS;
using Amazon.SQS.Model;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SQS backed <see cref="IInterBusTransport"/> for point-to-point queue delivery.
/// <para>
/// When <see cref="SendAsync"/> is called with a <c>sqsqueue://</c> destination URI it sends
/// the message directly to an SQS queue using
/// <see cref="SnsSqsTransportOptions.QueuePrefix"/><c>{queueName}</c> as the queue name,
/// delivering the message to exactly one consumer.
/// </para>
/// <para>
/// The message body is a JSON <see cref="SnsSqsMessageEnvelope"/> containing the full
/// set of InterBus headers and the base-64 encoded payload. A small subset of core
/// headers (<c>interbus-message-type</c>, <c>interbus-message-id</c>,
/// <c>interbus-correlation-id</c>) are additionally promoted to SQS message attributes so
/// that they are available for visibility without deserialising the envelope body.
/// </para>
/// <para>
/// Delayed delivery is supported via <see cref="TransportSendContext.Delay"/>. Amazon SQS
/// supports delay values from 0 to 900 seconds (15 minutes). Values exceeding 900 seconds
/// are clamped to 900 seconds.
/// </para>
/// <para>
/// Queue URLs are resolved once and cached for the application lifetime so that create calls
/// are not repeated on every send.
/// </para>
/// </summary>
public sealed class SqsTransport(IAmazonSQS sqsClient, SnsSqsClientProvider clientProvider) : IInterBusTransport
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
                $"Expected '{QueueScheme}'.",
                nameof(context));

        var rawQueueName = RoutingConventions.GetDestinationName(context.Destination);
        await SendToQueueAsync(rawQueueName, context, cancellationToken);
    }

    private async Task SendToQueueAsync(
        string rawQueueName,
        TransportSendContext context,
        CancellationToken ct)
    {
        var queueUrl = await GetOrCreateQueueUrlAsync(rawQueueName, ct);

        // Serialise the full payload + headers into the envelope body so that any
        // serialisation format (JSON, MessagePack, gzip, etc.) round-trips without
        // data loss, and header count is never limited by SQS's 10-attribute cap.
        var body = SnsSqsMessageEnvelope.Serialize(context.Payload, context.Headers);

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

    private async Task<string> GetOrCreateQueueUrlAsync(string rawQueueName, CancellationToken ct)
    {
        var queueName = clientProvider.BuildSqsQueueName(rawQueueName);

        if (clientProvider.QueueUrls.TryGetValue(queueName, out var cachedUrl))
            return cachedUrl;

        // CreateQueueAsync is idempotent on SQS: concurrent callers with the same name all
        // succeed and receive the same URL back.
        var response = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest
            {
                QueueName = queueName,
                Attributes = new Dictionary<string, string>
                {
                    ["MessageRetentionPeriod"] = "1209600", // 14 days (max)
                },
            },
            ct);

        var url = response.QueueUrl;
        clientProvider.QueueUrls.TryAdd(queueName, url);
        return url;
    }

    internal static Dictionary<string, Amazon.SQS.Model.MessageAttributeValue> BuildSqsMessageAttributes(
        MessageHeaders headers)
    {
        var result = new Dictionary<string, Amazon.SQS.Model.MessageAttributeValue>(
            StringComparer.Ordinal);

        foreach (var kv in headers)
        {
            if (!CoreAttributeHeaders.Contains(kv.Key))
                continue;

            result[kv.Key] = new Amazon.SQS.Model.MessageAttributeValue
            {
                DataType = "String",
                StringValue = kv.Value,
            };
        }

        return result;
    }
}
