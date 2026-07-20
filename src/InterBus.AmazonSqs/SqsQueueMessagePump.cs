using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs
{
    public partial class SqsQueueMessagePump(
        ConsumerDescriptor descriptor,
        IAmazonSQS sqsClient,
        SnsSqsTopologyBuilder topology,
        SnsSqsTransportOptions options,
        SqsPrefetchedMessageFactory prefetchedMessageFactory,
        Dispatcher dispatcher,
        ISnsSqsMessageEnvelope envelope,
        ILogger<SqsQueueMessagePump> logger)
        : IMessagePump
    {
        private readonly SemaphoreSlim _semaphore = new(descriptor.ConcurrencyCount);
        private readonly List<SqsPrefetchedMessage> _activeMessages = new(descriptor.ConcurrencyCount);
        
        private string? _queueUrl;
        private string? _deadLetterQueueUrl;

        public async Task InitializeAsync(CancellationToken stoppingToken)
        {
            _queueUrl = await topology.EnsureQueueAsync(descriptor.Queue, stoppingToken);
            _deadLetterQueueUrl = await topology.EnsureDeadLetterQueueAsync(descriptor.Queue, stoppingToken);
        }
        
        public async Task PumpLoopAsync(CancellationToken stoppingToken)
        {
            logger.LogDebug("InterBus AmazonSqs: consumer for queue {QueueName} started", _queueUrl);
            
            if (_queueUrl is null || _deadLetterQueueUrl is null)
                throw new InvalidOperationException("Queue URLs must be initialized before starting the pump");
            
            while (!stoppingToken.IsCancellationRequested)
            {
                await PumpAsync(_queueUrl, _deadLetterQueueUrl, stoppingToken);
            }

            logger.LogDebug("InterBus AmazonSqs: consumer for queue {QueueName} stopped", _queueUrl);
        }

        private async Task PumpAsync(string queueUrl, string deadLetterQueueUrl, CancellationToken stoppingToken)
        {
            var maxFetch = _semaphore.CurrentCount + options.PrefetchCount;

            if (maxFetch == 0)
            {
                await Task.Delay(10, stoppingToken);
                return;
            }

            List<Message> messages;
            try
            {
                LogPollingQueue(queueUrl, maxFetch);

                var response = await sqsClient.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MaxNumberOfMessages = Math.Min(maxFetch, 10),
                        WaitTimeSeconds = options.ReceiveWaitTimeSeconds,
                        MessageAttributeNames = ["All"],
                        VisibilityTimeout = (int)options.VisibilityTimeout.TotalSeconds,
                    },
                    stoppingToken);

                if (response?.Messages is null || response.Messages.Count == 0)
                {
                    LogNoMessagesReceived(queueUrl);
                    return;
                }

                messages = response.Messages;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus AmazonSqs: error polling queue {QueueName}; retrying",
                    queueUrl);

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                return;
            }

            LogReceivedMessageCount(messages.Count, queueUrl);

            var prefetchedMessages = messages
                .Select(message => prefetchedMessageFactory.Create(message, queueUrl, stoppingToken));

            foreach (var prefetched in prefetchedMessages)
            {
                _activeMessages.Add(prefetched);
                await _semaphore.WaitAsync(stoppingToken);

                prefetched.AttachDispatchTask(ProcessMessageAsync(queueUrl, deadLetterQueueUrl, prefetched,
                    stoppingToken));
            }

            _activeMessages.RemoveAll(m => m.DispatchCompleted());
        }

        private async Task ProcessMessageAsync(
            string queueUrl,
            string deadLetterQueueUrl,
            SqsPrefetchedMessage prefetched,
            CancellationToken ct)
        {
            var message = prefetched.Message;

            try
            {
                var result = await DispatchAsync(message, ct);
                switch (result.Kind)
                {
                    case DispatchResultKind.Success:
                        LogDispatchedSuccessfully(message.MessageId, descriptor.Queue);

                        // ACK: delete the message from SQS.
                        // Use CancellationToken.None so that a graceful shutdown does not leave
                        // successfully dispatched messages to be redelivered unnecessarily.
                        await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle,
                            CancellationToken.None);
                        break;
                    case DispatchResultKind.DeadLetter:
                        if (await TryDeadLetterAsync(deadLetterQueueUrl, message, result))
                            await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None);
                        break;
                    case DispatchResultKind.Release:
                        LogConsumerFailed(descriptor.ConsumerType.Name, descriptor.Queue, result.Exception);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Dispatch was cancelled mid-flight, SQS will redeliver
            }
            catch (Exception ex)
            {
                LogConsumerFailed(descriptor.ConsumerType.Name, descriptor.Queue, ex);
            }
            finally
            {
                await prefetched.StopRenewalAsync();
                _semaphore.Release();
            }
        }

        private async Task<DispatchResult> DispatchAsync(Message message,
            CancellationToken ct)
        {
            if (message.Body is null)
                throw new InvalidOperationException(
                    $"SQS message {message.MessageId} has a null body and cannot be deserialised");

            var (payload, headers) = envelope.DeserializeFromSqs(message.Body);

            var context = new DispatchContext(descriptor, payload, headers);
            await dispatcher.DispatchAsync(context, ct);
            return context.Result;
        }

        private async Task<bool> TryDeadLetterAsync(string deadLetterQueueUrl, Message message, DispatchResult result)
        {
            try
            {
                var deadLetterMessageAttributes = message.MessageAttributes ??
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

                await sqsClient.SendMessageAsync(
                    new SendMessageRequest
                    {
                        QueueUrl = deadLetterQueueUrl,
                        MessageBody = message.Body,
                        MessageAttributes = deadLetterMessageAttributes
                    },
                    CancellationToken.None);

                logger.LogWarning(
                    result.Exception,
                    "InterBus AmazonSqs: consumer {ConsumerType} failed for queue {QueueName}; message moved to dead-letter queue {DeadLetterQueue}",
                    descriptor.ConsumerType.Name,
                    descriptor.Queue,
                    deadLetterQueueUrl);

                return true;
            }
            catch (Exception deadLetterFailure)
            {
                logger.LogError(
                    deadLetterFailure,
                    "InterBus AmazonSqs: failed sending dead-letter message for consumer {ConsumerType} queue {QueueName}",
                    descriptor.ConsumerType.Name,
                    descriptor.Queue);
                return false;
            }
        }

        [LoggerMessage(LogLevel.Trace,
            "InterBus AmazonSqs: message {MessageId} dispatched successfully; deleting from queue {QueueName}")]
        partial void LogDispatchedSuccessfully(string messageId, Uri queueName);

        [LoggerMessage(LogLevel.Warning,
            "InterBus AmazonSqs: consumer {ConsumerType} failed for queue {QueueName}; message will be redelivered after visibility timeout")]
        partial void LogConsumerFailed(string consumerType, Uri queueName, Exception? exception);

        [LoggerMessage(LogLevel.Trace, "InterBus AmazonSqs: no messages received from queue {QueueName}")]
        partial void LogNoMessagesReceived(string queueName);

        [LoggerMessage(LogLevel.Trace, "InterBus AmazonSqs: received {Count} message(s) from queue {QueueName}")]
        partial void LogReceivedMessageCount(int count, string queueName);

        [LoggerMessage(LogLevel.Trace, "InterBus AmazonSqs: polling queue {QueueName} (maxFetch={MaxFetch})")]
        partial void LogPollingQueue(string queueName, int maxFetch);
    }
}
