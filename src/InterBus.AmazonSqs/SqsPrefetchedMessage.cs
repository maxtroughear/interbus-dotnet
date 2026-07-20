using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

public sealed class SqsPrefetchedMessage : IDisposable
{
    public Message Message { get; }

    private readonly ILogger<SqsPrefetchedMessage> _logger;
    private readonly IAmazonSQS _sqsClient;
    private readonly string _queueUrl;
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
        CancellationToken stoppingToken)
    {
        Message = message;
        _logger = logger;
        _sqsClient = sqsClient;
        _extensionInterval = options.EffectiveLeaseRenewalInterval;
        _visibilityTimeout = Convert.ToInt32(options.VisibilityTimeout.TotalSeconds);
        _queueUrl = queueUrl;
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
            catch (ReceiptHandleIsInvalidException) when (DispatchCompleted())
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
}
