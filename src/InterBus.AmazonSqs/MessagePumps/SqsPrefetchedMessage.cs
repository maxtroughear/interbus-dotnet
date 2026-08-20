using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.MessagePumps;

public sealed class SqsPrefetchedMessage : IDisposable, IMessage
{
    public Message Message { get; }

    private readonly ILogger<SqsPrefetchedMessage> _logger;
    private readonly IAmazonSQS _sqsClient;
    private readonly string _queueUrl;
    private readonly string _deadLetterQueueUrl;
    private readonly TimeSpan _extensionInterval;
    private readonly int _visibilityTimeout;
    private readonly CancellationTokenSource _renewalCts;
    private readonly Task _renewalTask;
    private Task? _dispatchTask;

    public SqsPrefetchedMessage(
        ILogger<SqsPrefetchedMessage> logger,
        IAmazonSQS sqsClient,
        SnsSqsTransportOptions options,
        Message message,
        string queueUrl,
        string deadLetterQueueUrl,
        CancellationToken stoppingToken)
    {
        Message = message;
        _logger = logger;
        _sqsClient = sqsClient;
        _extensionInterval = options.EffectiveLeaseRenewalInterval;
        _visibilityTimeout = Convert.ToInt32(options.VisibilityTimeout.TotalSeconds);
        _queueUrl = queueUrl;
        _deadLetterQueueUrl = deadLetterQueueUrl;
        _renewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _renewalTask = ExtendVisibilityAsync();
    }

    public async Task StopRenewalAsync()
    {
        try
        {
            await _renewalCts.CancelAsync();
            await _renewalTask;
        }
        catch
        {
            // ignore
        }
    }

    public void AttachDispatchTask(Task dispatchTask)
    {
        _dispatchTask = dispatchTask;
    }

    public bool DispatchCompleted()
    {
        return _dispatchTask?.IsCompleted ?? false;
    }

    private async Task ExtendVisibilityAsync()
    {
        while (!_renewalCts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_extensionInterval, _renewalCts.Token);

                await _sqsClient.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = _queueUrl,
                    ReceiptHandle = Message.ReceiptHandle,
                    VisibilityTimeout = _visibilityTimeout
                }, _renewalCts.Token);

                _logger.LogTrace(
                    "InterBus AmazonSqs: extended visibility timeout for message (receipt={ShortHandle}) by {Seconds}s",
                    Message.ReceiptHandle[..Math.Min(8, Message.ReceiptHandle.Length)],
                    _visibilityTimeout);
            }
            catch (ReceiptHandleIsInvalidException)
            {
                // Message has been processed and most likely deleted
                break;
            }
            catch (OperationCanceledException)
            {
                // Cancellation requested, exit the loop
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "InterBus AmazonSqs: failed to extend visibility timeout for message (receipt={ShortHandle})",
                    Message.ReceiptHandle[..Math.Min(8, Message.ReceiptHandle.Length)]);
            }
        }
    }

    public void Dispose()
    {
        _renewalCts.Dispose();
    }

    public async Task Acknowledge(CancellationToken cancellationToken)
    {
        await _sqsClient.DeleteMessageAsync(_queueUrl, Message.ReceiptHandle, cancellationToken);
        await StopRenewalAsync();
    }

    public async Task Abandon(CancellationToken cancellationToken)
    {
        await StopRenewalAsync();
        try
        {
            await _sqsClient.ChangeMessageVisibilityAsync(_queueUrl, Message.ReceiptHandle, 0, cancellationToken);
        }
        catch (ReceiptHandleIsInvalidException)
        {
            _logger.LogWarning(
                "InterBus AmazonSqs: Failed to abandon message, receipt handle is invalid, message may have been deleted or handle expired (receipt={ShortHandle}",
                Message.ReceiptHandle[..Math.Min(8, Message.ReceiptHandle.Length)]);
        }
    }

    public async Task DeadLetter(DispatchResult result, CancellationToken cancellationToken)
    {
        var deadLetterMessageAttributes = Message.MessageAttributes ??
                                          new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        deadLetterMessageAttributes[InterBusHeaders.DeadLetterReason] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = result.DeadLetterReason
        };
        deadLetterMessageAttributes[InterBusHeaders.DeadLetterError] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = result.DeadLetterError
        };
        deadLetterMessageAttributes[InterBusHeaders.DeadLetterTime] = new MessageAttributeValue
        {
            DataType = "String",
            StringValue = result.DeadLetteredAtUtc!.Value.ToString("O")
        };

        await _sqsClient.SendMessageAsync(
            new SendMessageRequest
            {
                QueueUrl = _deadLetterQueueUrl,
                MessageBody = Message.Body,
                MessageAttributes = deadLetterMessageAttributes
            },
            cancellationToken);

        await _sqsClient.DeleteMessageAsync(_queueUrl, Message.ReceiptHandle, cancellationToken);
        
        await StopRenewalAsync();
        
        // TODO: Move logging into InterBus core
        // _logger.LogWarning(
        //     result.Exception,
        //     "InterBus AmazonSqs: consumer {ConsumerType} failed for queue {QueueName}; message moved to dead-letter queue {DeadLetterQueue}",
        //     Descriptor.ConsumerType.Name,
        //     descriptor.Queue,
        //     deadLetterQueueUrl);
        // TODO: Move error handling into InterBus core
        // catch (Exception deadLetterFailure)
        // {
        //     _logger.LogError(
        //         deadLetterFailure,
        //         "InterBus AmazonSqs: failed sending dead-letter message for consumer {ConsumerType} queue {QueueName}",
        //         descriptor.ConsumerType.Name,
        //         descriptor.Queue);
        //     return false;
        // }
    }
}
