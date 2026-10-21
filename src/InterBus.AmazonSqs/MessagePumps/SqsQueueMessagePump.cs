using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.MessagePumps;

internal sealed class SqsQueueMessagePump(
    ConsumerDescriptor descriptor,
    IAmazonSQS sqsClient,
    SnsSqsTopologyBuilder topology,
    SnsSqsNaming naming,
    SnsSqsTransportOptions options,
    ISnsSqsMessageEnvelope envelope,
    ILoggerFactory loggerFactory,
    ILogger<SqsQueueMessagePump> logger) : IBufferedPollingSource, IPollingRetryPolicy
{
    private string _queueUrl = null!;
    private string _deadLetterQueueUrl = null!;
    public int PrefetchCount => options.PrefetchCount;

    public Task InitializeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var queueName = naming.BuildSqsQueueName(descriptor.Queue);
        _queueUrl = topology.QueueUrls.TryGetValue(queueName, out var url) ? url
            : throw new InvalidOperationException($"Queue topology not initialized: {queueName}");
        return InitializeDeadLetterAsync(cancellationToken);
    }

    private async Task InitializeDeadLetterAsync(CancellationToken cancellationToken) =>
        _deadLetterQueueUrl = await topology.EnsureDeadLetterQueueAsync(descriptor.Queue, cancellationToken);

    public int MaxBatchSize => 10;
    public TimeSpan? GetRetryDelay(Exception error, int attempt)
    {
        if (error is not ReceiveException) return null;
        logger.LogWarning(error.InnerException, "InterBus SQS: receive failed for {Queue}; retrying", _queueUrl);
        return TimeSpan.FromSeconds(5);
    }

    public async Task<PollResult> ReceiveOnceAsync(ReceiveBatch batch, CancellationToken intakeToken)
    {
        var deliveries = new List<SqsPrefetchedMessage>();
        try
        {
            ReceiveMessageResponse response;
            try
            {
                response = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _queueUrl,
                    MaxNumberOfMessages = batch.Capacity,
                    WaitTimeSeconds = options.ReceiveWaitTimeSeconds,
                    VisibilityTimeout = (int)options.VisibilityTimeout.TotalSeconds,
                    MessageAttributeNames = ["All"],
                    MessageSystemAttributeNames = ["MessageGroupId", "MessageDeduplicationId"]
                }, intakeToken);
            }
            catch (OperationCanceledException) when (intakeToken.IsCancellationRequested) { throw; }
            catch (Exception error) { throw new ReceiveException(error); }
            // Start renewal for the entire response before handing any delivery to processing
            foreach (var message in response.Messages ?? [])
                deliveries.Add(new SqsPrefetchedMessage(loggerFactory.CreateLogger<SqsPrefetchedMessage>(),
                    sqsClient, options, message, _queueUrl, _deadLetterQueueUrl, envelope));
            for (var i = 0; i < deliveries.Count; i++)
            {
                intakeToken.ThrowIfCancellationRequested();
                batch.Transfer(deliveries[i]);
                deliveries[i] = null!;
            }
            return new PollResult(deliveries.Count != 0);
        }
        finally
        {
            List<Exception>? failures = null;
            foreach (var delivery in deliveries)
            {
                if (delivery is null) continue;
                try { await delivery.DisposeAsync(); }
                catch (Exception error) { (failures ??= []).Add(error); }
            }
            if (failures is not null) throw new AggregateException("SQS batch cleanup failed", failures);
        }
    }

    private sealed class ReceiveException(Exception error) : Exception("SQS receive failed", error);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
