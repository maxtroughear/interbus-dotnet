using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.MessagePumps;

internal sealed class SqsPrefetchedMessage : IDelivery
{
    private readonly IAmazonSQS _client;
    private readonly Message _message;
    private readonly string _queueUrl;
    private readonly string _deadLetterQueueUrl;
    private readonly CancellationTokenSource _renewal = new();
    private readonly Task _renewalTask;
    private volatile bool _lost;
    public ReadOnlyMemory<byte> Payload { get; }
    public MessageHeaders Headers { get; } = new();
    public Exception? DecodeError { get; }
    public string? OrderingKey => _message.Attributes?.GetValueOrDefault("MessageGroupId");

    public SqsPrefetchedMessage(ILogger<SqsPrefetchedMessage> logger, IAmazonSQS client,
        SnsSqsTransportOptions options, Message message, string queueUrl, string deadLetterQueueUrl,
        ISnsSqsMessageEnvelope envelope)
    {
        _client = client;
        _message = message;
        _queueUrl = queueUrl;
        _deadLetterQueueUrl = deadLetterQueueUrl;
        _renewalTask = RenewAsync();
        try
        {
            if (message.Body is null) throw new FormatException("SQS message body is null");
            if (envelope is SnsSerialisedMessageEnvelope)
            {
                using var document = System.Text.Json.JsonDocument.Parse(message.Body);
                if (document.RootElement.TryGetProperty("p", out _)) envelope = new SnsSqsMessageEnvelope();
            }
            var decoded = envelope.DeserializeFromSqs(message.Body);
            Payload = decoded.Payload;
            Headers = decoded.Headers;
        }
        catch (Exception error) { DecodeError = error; }

        async Task RenewAsync()
        {
            try
            {
                while (true)
                {
                    await Task.Delay(options.EffectiveLeaseRenewalInterval, _renewal.Token);
                    await client.ChangeMessageVisibilityAsync(queueUrl, message.ReceiptHandle,
                        (int)options.VisibilityTimeout.TotalSeconds, _renewal.Token);
                }
            }
            catch (OperationCanceledException) when (_renewal.IsCancellationRequested) { }
            catch (Exception error)
            {
                _lost = true;
                logger.LogWarning(error, "InterBus SQS: ownership renewal failed for {MessageId}", message.MessageId);
            }
        }
    }

    public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
    {
        if (_lost) return false;
        switch (result.Kind)
        {
            case DispatchResultKind.Success:
                await _client.DeleteMessageAsync(_queueUrl, _message.ReceiptHandle, cancellationToken);
                break;
            case DispatchResultKind.Release:
                // Preserve visibility-expiry redelivery rather than making the message immediately visible
                await _renewal.CancelAsync();
                await _renewalTask;
                break;
            case DispatchResultKind.DeadLetter:
                var attributes = new Dictionary<string, MessageAttributeValue>(_message.MessageAttributes ?? []);
                attributes[InterBusHeaders.DeadLetterReason] = Attribute(result.DeadLetterReason);
                attributes[InterBusHeaders.DeadLetterError] = Attribute(result.DeadLetterError);
                attributes[InterBusHeaders.DeadLetterTime] = Attribute(result.DeadLetteredAtUtc!.Value.ToString("O"));
                await _client.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = _deadLetterQueueUrl,
                    MessageBody = _message.Body,
                    MessageAttributes = attributes,
                    MessageGroupId = OrderingKey,
                    MessageDeduplicationId = _deadLetterQueueUrl.EndsWith(".fifo", StringComparison.OrdinalIgnoreCase)
                        ? Guid.NewGuid().ToString("N") : null
                }, cancellationToken);
                await _client.DeleteMessageAsync(_queueUrl, _message.ReceiptHandle, cancellationToken);
                break;
            default: throw new ArgumentOutOfRangeException(nameof(result));
        }
        return true;
    }

    private static MessageAttributeValue Attribute(string value) => new() { DataType = "String", StringValue = value };

    public async ValueTask DisposeAsync()
    {
        await _renewal.CancelAsync();
        try { await _renewalTask; }
        finally { _renewal.Dispose(); }
    }
}
