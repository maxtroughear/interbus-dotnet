using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

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
/// InterBus message headers are transmitted as SNS message attributes (string type). SNS
/// supports up to 10 message attributes per message; ensure the number of InterBus headers
/// does not exceed this limit.
/// </para>
/// <para>
/// Topic ARNs are resolved once and cached for the application lifetime so that create calls
/// are not repeated on every send.
/// </para>
/// <para>
/// Delayed delivery is not supported for SNS topic publishes; any
/// <see cref="TransportSendContext.Delay"/> value is ignored.
/// </para>
/// </summary>
public sealed class SnsTransport : IInterBusTransport
{
    /// <summary>URI scheme for SNS topic (fan-out) destinations.</summary>
    public const string TopicScheme = "snstopic";

    private readonly IAmazonSimpleNotificationService _snsClient;
    private readonly SnsSqsClientProvider _clientProvider;

    /// <summary>
    /// Initialises a new instance of <see cref="SnsTransport"/>.
    /// </summary>
    public SnsTransport(IAmazonSimpleNotificationService snsClient, SnsSqsClientProvider clientProvider)
    {
        _snsClient = snsClient;
        _clientProvider = clientProvider;
    }

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
        var topicArn = await GetOrCreateTopicArnAsync(rawTopicName, ct);
        var messageAttributes = BuildSnsMessageAttributes(context.Headers);
        var body = System.Text.Encoding.UTF8.GetString(context.Payload);

        await _snsClient.PublishAsync(
            new PublishRequest
            {
                TopicArn = topicArn,
                Message = body,
                MessageAttributes = messageAttributes,
            },
            ct);
    }

    private async Task<string> GetOrCreateTopicArnAsync(string rawTopicName, CancellationToken ct)
    {
        var topicName = _clientProvider.BuildSnsTopicName(rawTopicName);

        if (_clientProvider.TopicArns.TryGetValue(topicName, out var cachedArn))
            return cachedArn;

        var response = await _snsClient.CreateTopicAsync(topicName, ct);
        var arn = response.TopicArn;
        _clientProvider.TopicArns.TryAdd(topicName, arn);
        return arn;
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
}
