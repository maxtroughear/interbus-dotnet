using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.MessagePumps
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

        private readonly Channel<SqsPrefetchedMessage> _channel =
            Channel.CreateBounded<SqsPrefetchedMessage>(new BoundedChannelOptions(options.PrefetchCount)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = false,
            });

        private readonly List<Task> _channelLoopTasks = [];

        private string? _queueUrl;
        private string? _deadLetterQueueUrl;

        public bool SelfContained => false;

        public async Task InitializeAsync(CancellationToken stoppingToken)
        {
            _queueUrl = await topology.EnsureQueueAsync(descriptor.Queue, stoppingToken);
            _deadLetterQueueUrl = await topology.EnsureDeadLetterQueueAsync(descriptor.Queue, stoppingToken);

            for (var i = 0; i < descriptor.ConcurrencyCount; i++)
            {
                _channelLoopTasks.Add(ChannelLoopAsync(stoppingToken));
            }
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

            var prefetchedMessages = messages.Select(message =>
                prefetchedMessageFactory.Create(message, queueUrl, deadLetterQueueUrl, stoppingToken));

            foreach (var prefetched in prefetchedMessages)
            {
                await _channel.Writer.WriteAsync(prefetched, stoppingToken);
            }
        }

        private async Task ChannelLoopAsync(CancellationToken stoppingToken)
        {
            await foreach (var message in _channel.Reader.ReadAllAsync(stoppingToken))
            {
                try
                {
                    var result = await DispatchAsync(message.Message, stoppingToken);

                    // log result
                    LogDispatchedSuccessfully(message.Message.MessageId, descriptor.Queue);
                    LogConsumerFailed(descriptor.ConsumerType.Name, descriptor.Queue, result.Exception);
                    // Log Dead Letter

                    await (result.Kind switch
                    {
                        DispatchResultKind.Success => message.Acknowledge(CancellationToken.None),
                        DispatchResultKind.DeadLetter => message.DeadLetter(result, CancellationToken.None),
                        DispatchResultKind.Release => message.Abandon(CancellationToken.None),
                        _ => throw new ArgumentOutOfRangeException(),
                    });
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Dispatch was cancelled mid-flight, SQS will redeliver
                }
                catch (Exception ex)
                {
                    LogConsumerFailed(descriptor.ConsumerType.Name, descriptor.Queue, ex);
                }
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
