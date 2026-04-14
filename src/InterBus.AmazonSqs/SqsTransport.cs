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
/// InterBus message headers are transmitted as SQS message attributes (string type). SQS
/// supports up to 10 message attributes per message; ensure the number of InterBus headers
/// does not exceed this limit.
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
        var messageAttributes = BuildSqsMessageAttributes(context.Headers);

        // Base64-encode the raw payload bytes so that any serialisation format
        // (JSON, MessagePack, gzip-compressed, etc.) round-trips without data loss
        // through the SQS string message body.
        var body = Convert.ToBase64String(context.Payload);

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
            headers.Count,
            StringComparer.Ordinal);

        foreach (var kv in headers)
        {
            result[kv.Key] = new Amazon.SQS.Model.MessageAttributeValue
            {
                DataType = "String",
                StringValue = kv.Value,
            };
        }

        return result;
    }

    internal static MessageHeaders ParseSqsMessageAttributes(
        Dictionary<string, Amazon.SQS.Model.MessageAttributeValue>? sqsAttributes)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);

        if (sqsAttributes is not null)
        {
            foreach (var kv in sqsAttributes)
            {
                if (kv.Value.DataType == "String" && kv.Value.StringValue is not null)
                    dict[kv.Key] = kv.Value.StringValue;
            }
        }

        return new MessageHeaders(dict);
    }
}
