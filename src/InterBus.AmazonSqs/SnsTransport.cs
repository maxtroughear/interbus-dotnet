using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SNS/SQS backed <see cref="IInterBusTransport"/>.
/// <para>
/// When <see cref="SendAsync"/> is called with a <c>snstopic://</c> destination URI it
/// publishes the message to an SNS topic using
/// <see cref="SnsSqsTransportOptions.TopicPrefix"/><c>{topicName}</c> as the topic name.
/// SNS fans the message out to every SQS queue subscribed to that topic, enabling the
/// publish/subscribe pattern.
/// </para>
/// <para>
/// When called with a <c>sqsqueue://</c> destination URI it sends the message directly to
/// an SQS queue using <see cref="SnsSqsTransportOptions.QueuePrefix"/><c>{queueName}</c>
/// as the queue name, delivering it to exactly one consumer.
/// </para>
/// <para>
/// InterBus message headers are transmitted as SQS/SNS message attributes (string type).
/// SNS and SQS each support up to 10 message attributes per message; ensure that the
/// combined number of InterBus and user-defined headers does not exceed this limit.
/// </para>
/// <para>
/// Delayed delivery is supported for direct SQS sends via
/// <see cref="TransportSendContext.Delay"/>. Amazon SQS supports delay values from 0 to
/// 900 seconds (15 minutes). Values exceeding 900 seconds are clamped to 900 seconds.
/// SNS topic publishes do not support delay; the delay value is ignored.
/// </para>
/// <para>
/// Topic ARNs and queue URLs are resolved once and cached for the application lifetime so
/// that describe/create calls are not repeated on every send.
/// </para>
/// </summary>
public sealed class SnsTransport : IInterBusTransport
{
    /// <summary>URI scheme for SNS topic (fan-out) destinations.</summary>
    public const string TopicScheme = "snstopic";

    /// <summary>URI scheme for SQS queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "sqsqueue";

    private readonly SnsSqsClientProvider _clientProvider;

    internal SnsTransport(SnsSqsClientProvider clientProvider)
    {
        _clientProvider = clientProvider;
    }

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var destinationName = RoutingConventions.GetDestinationName(context.Destination);

        if (context.Destination.Scheme == TopicScheme)
        {
            await PublishToTopicAsync(destinationName, context, cancellationToken);
        }
        else if (context.Destination.Scheme == QueueScheme)
        {
            await SendToQueueAsync(destinationName, context, cancellationToken);
        }
        else
        {
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{TopicScheme}' or '{QueueScheme}'.",
                nameof(context));
        }
    }

    private async Task PublishToTopicAsync(
        string rawTopicName,
        TransportSendContext context,
        CancellationToken ct)
    {
        var topicArn = await GetOrCreateTopicArnAsync(rawTopicName, ct);
        var messageAttributes = BuildSnsMessageAttributes(context.Headers);
        var body = System.Text.Encoding.UTF8.GetString(context.Payload);

        await _clientProvider.SnsClient.PublishAsync(
            new PublishRequest
            {
                TopicArn = topicArn,
                Message = body,
                MessageAttributes = messageAttributes,
            },
            ct);
    }

    private async Task SendToQueueAsync(
        string rawQueueName,
        TransportSendContext context,
        CancellationToken ct)
    {
        var queueUrl = await GetOrCreateQueueUrlAsync(rawQueueName, ct);
        var messageAttributes = BuildSqsMessageAttributes(context.Headers);
        var body = System.Text.Encoding.UTF8.GetString(context.Payload);

        // SQS supports delay of 0-900 seconds; clamp accordingly.
        var delaySeconds = context.Delay.HasValue
            ? (int)Math.Clamp(context.Delay.Value.TotalSeconds, 0, 900)
            : 0;

        await _clientProvider.SqsClient.SendMessageAsync(
            new SendMessageRequest
            {
                QueueUrl = queueUrl,
                MessageBody = body,
                MessageAttributes = messageAttributes,
                DelaySeconds = delaySeconds,
            },
            ct);
    }

    private async Task<string> GetOrCreateTopicArnAsync(string rawTopicName, CancellationToken ct)
    {
        var topicName = _clientProvider.BuildSnsTopicName(rawTopicName);

        if (_clientProvider.TopicArns.TryGetValue(topicName, out var cachedArn))
            return cachedArn;

        var response = await _clientProvider.SnsClient.CreateTopicAsync(topicName, ct);
        var arn = response.TopicArn;
        _clientProvider.TopicArns.TryAdd(topicName, arn);
        return arn;
    }

    private async Task<string> GetOrCreateQueueUrlAsync(string rawQueueName, CancellationToken ct)
    {
        var queueName = _clientProvider.BuildSqsQueueName(rawQueueName);

        if (_clientProvider.QueueUrls.TryGetValue(queueName, out var cachedUrl))
            return cachedUrl;

        var response = await _clientProvider.SqsClient.CreateQueueAsync(
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
        _clientProvider.QueueUrls.TryAdd(queueName, url);
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

    internal static Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>
        BuildSnsMessageAttributes(MessageHeaders headers)
    {
        var result =
            new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>(
                headers.Count,
                StringComparer.Ordinal);

        foreach (var kv in headers)
        {
            result[kv.Key] =
                new Amazon.SimpleNotificationService.Model.MessageAttributeValue
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
