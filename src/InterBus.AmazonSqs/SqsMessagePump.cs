using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consuming;
using InterBus.DeadLettering;
using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that polls each
/// registered consumer's SQS queue and dispatches messages to the InterBus consume pipeline.
/// <para>
/// Topology setup (creating SNS topics, SQS queues, and subscriptions) is handled during bus
/// startup by <see cref="SnsSqsTopologyBuilder"/> before polling begins.
/// </para>
/// <para>
/// Messages delivered via SNS can use either raw message delivery
/// (<see cref="SnsSqsMessageEnvelope"/>) or SNS-serialised notifications on SQS
/// (<c>RawMessageDelivery=false</c>) based on transport/consumer configuration.
/// Direct sends (<c>sqsqueue://</c>) always use the InterBus envelope format.
/// </para>
/// <para>
/// Successful dispatch deletes the SQS message (ACK). Unhandled consumer failures are copied to
/// the configured dead-letter queue with diagnostic message attributes and the original message is
/// acknowledged; other failures let visibility timeout expire for redelivery.
/// </para>
/// </summary>
internal sealed class SqsMessagePump(
    IAmazonSQS sqsClient,
    SnsSqsTopology topology,
    SnsSqsTransportOptions options,
    InterBusOptions busOptions,
    IServiceScopeFactory scopeFactory,
    ILogger<SqsMessagePump> logger)
    : BackgroundService
{
    private sealed class PrefetchedMessage(
        Message message,
        CancellationTokenSource renewalCts,
        Task renewalTask)
    {
        public Message Message { get; } = message;
        public CancellationTokenSource RenewalCts { get; } = renewalCts;
        public Task RenewalTask { get; } = renewalTask;
        public Task? DispatchTask { get; set; }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = busOptions.Consumers;
        if (consumers.Count == 0)
        {
            logger.LogInformation("InterBus AmazonSqs: no consumers registered; message pump idle");
            return;
        }

        var tasks = consumers.Select(d => ConsumeQueueAsync(d, stoppingToken));
        await Task.WhenAll(tasks);
    }

    private async Task ConsumeQueueAsync(ConsumerDescriptor descriptor, CancellationToken stoppingToken)
    {
        var queueName = topology.BuildSqsQueueName(descriptor.QueueName);

        if (!topology.QueueUrls.TryGetValue(queueName, out var queueUrl))
        {
            logger.LogError(
                "InterBus AmazonSqs: queue URL not found for {QueueName}; consumer {ConsumerType} will not start",
                queueName,
                descriptor.ConsumerType.Name);
            return;
        }

        logger.LogDebug(
            "InterBus AmazonSqs: starting consumer for queue {QueueName}",
            queueName);

        var concurrency = Math.Max(1, descriptor.ConcurrencyCount);

        using var semaphore = new SemaphoreSlim(concurrency);
        var activeMessages = new List<PrefetchedMessage>();

        while (!stoppingToken.IsCancellationRequested)
        {
            // Determine how many messages to fetch based on current processing capacity.
            var maxFetch = semaphore.CurrentCount + options.PrefetchCount;

            if (maxFetch == 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10), stoppingToken);
                continue;
            }

            List<Message> messages;
            try
            {
                logger.LogTrace(
                    "InterBus AmazonSqs: polling queue {QueueName} (maxFetch={MaxFetch})",
                    queueName,
                    maxFetch);

                var response = await sqsClient.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MaxNumberOfMessages = Math.Clamp(maxFetch, 1, 10),
                        WaitTimeSeconds = options.ReceiveWaitTimeSeconds,
                        MessageAttributeNames = ["All"],
                        VisibilityTimeout = (int)options.VisibilityTimeout.TotalSeconds,
                    },
                    stoppingToken);

                if (response?.Messages is null || !response.Messages.Any())
                {
                    logger.LogTrace(
                        "InterBus AmazonSqs: no messages received from queue {QueueName}",
                        queueName);
                    continue;
                }

                messages = response.Messages;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus AmazonSqs: error polling queue {QueueName}; retrying",
                    queueName);

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                continue;
            }

            logger.LogTrace(
                "InterBus AmazonSqs: received {Count} message(s) from queue {QueueName}",
                messages.Count,
                queueName);

            var prefetchedMessages =
                messages.Select(message => StartPrefetchedMessage(queueUrl, message, stoppingToken))
                    .ToList();

            activeMessages.AddRange(prefetchedMessages);

            foreach (var prefetched in prefetchedMessages)
            {
                await semaphore.WaitAsync(stoppingToken);

                prefetched.DispatchTask =
                    ProcessMessageAsync(descriptor, queueUrl, prefetched, semaphore, stoppingToken);
            }

            activeMessages.RemoveAll(m => m.DispatchTask?.IsCompleted ?? false);
        }

        if (activeMessages.Count > 0)
        {
            try
            {
                await Task.WhenAll(activeMessages.Select(m => m.DispatchTask ?? StopRenewalAsync(m)));
            }
            catch
            {
                // Exceptions are already handled (logged) inside ProcessMessageAsync.
            }
        }

        logger.LogDebug(
            "InterBus AmazonSqs: consumer for queue {QueueName} stopped",
            queueName);
    }

    private async Task ProcessMessageAsync(
        ConsumerDescriptor descriptor,
        string queueUrl,
        PrefetchedMessage prefetched,
        SemaphoreSlim semaphore,
        CancellationToken ct)
    {
        var message = prefetched.Message;

        logger.LogTrace(
            "InterBus AmazonSqs: processing message {MessageId} from queue {QueueName} (body length={BodyLength}, attributes={AttributeCount})",
            message.MessageId,
            descriptor.Queue,
            message.Body?.Length ?? 0,
            message.MessageAttributes?.Count ?? 0);

        try
        {
            await DispatchAsync(descriptor, message, ct);

            await prefetched.RenewalCts.CancelAsync();

            logger.LogTrace(
                "InterBus AmazonSqs: message {MessageId} dispatched successfully; deleting from queue {QueueName}",
                message.MessageId,
                descriptor.Queue);

            // ACK: delete the message from SQS.
            // Use CancellationToken.None so that a graceful shutdown does not leave
            // successfully-dispatched messages to be redelivered unnecessarily.
            await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle,
                CancellationToken.None);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Dispatch was cancelled mid-flight; do not delete – SQS will redeliver.
        }
        catch (DeadLetterException ex)
        {
            if (await TryDeadLetterAsync(descriptor, message, ex))
                await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "InterBus AmazonSqs: consumer {ConsumerType} failed for queue {QueueName}; message will be redelivered after visibility timeout",
                descriptor.ConsumerType.Name,
                descriptor.Queue);
        }
        finally
        {
            try
            {
                await StopRenewalAsync(prefetched);
            }
            finally
            {
                semaphore.Release();
            }
        }
    }

    private async Task<bool> TryDeadLetterAsync(ConsumerDescriptor descriptor, Message message, DeadLetterException ex)
    {
        var deadLetterQueueName = topology.BuildDeadLetterQueueName(descriptor.QueueName);
        if (!topology.QueueUrls.TryGetValue(deadLetterQueueName, out var deadLetterQueueUrl))
        {
            logger.LogWarning(
                ex,
                "InterBus AmazonSqs: failed to dead-letter message for consumer {ConsumerType}; dead-letter queue {DeadLetterQueue} is not available",
                descriptor.ConsumerType.Name,
                deadLetterQueueName);
            return false;
        }

        try
        {
            var deadLetterMessageAttributes = message.MessageAttributes ??
                                              new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
            deadLetterMessageAttributes[InterBusHeaders.DeadLetterReason] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = ex.Reason
            };
            deadLetterMessageAttributes[InterBusHeaders.DeadLetterError] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = ex.Error
            };
            deadLetterMessageAttributes[InterBusHeaders.DeadLetteredAt] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = ex.DeadLetteredAtUtc.ToString("O")
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
                ex,
                "InterBus AmazonSqs: consumer {ConsumerType} failed for queue {QueueName}; message moved to dead-letter queue {DeadLetterQueue}",
                descriptor.ConsumerType.Name,
                descriptor.Queue,
                deadLetterQueueName);

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

    private static async Task StopRenewalAsync(PrefetchedMessage prefetched)
    {
        await prefetched.RenewalCts.CancelAsync();

        try
        {
            await prefetched.RenewalTask;
        }
        catch
        {
            // ignored
        }

        prefetched.RenewalCts.Dispose();
    }

    private PrefetchedMessage StartPrefetchedMessage(
        string queueUrl,
        Message message,
        CancellationToken stoppingToken)
    {
        var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var renewalTask = ExtendVisibilityAsync(queueUrl, message.ReceiptHandle, renewalCts.Token);
        return new PrefetchedMessage(message, renewalCts, renewalTask);
    }

    /// <summary>
    /// Periodically extends the visibility timeout of an in-flight SQS message so that
    /// long-running consumers do not cause inadvertent redelivery. Fires at
    /// <see cref="SnsSqsTransportOptions.EffectiveLeaseRenewalInterval"/> until
    /// <paramref name="ct"/> is cancelled.
    /// </summary>
    private async Task ExtendVisibilityAsync(string queueUrl, string receiptHandle, CancellationToken ct)
    {
        var extensionInterval = options.EffectiveLeaseRenewalInterval;
        var visibilitySeconds = (int)options.VisibilityTimeout.TotalSeconds;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(extensionInterval, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (ct.IsCancellationRequested)
                break;

            try
            {
                await sqsClient.ChangeMessageVisibilityAsync(
                    queueUrl,
                    receiptHandle,
                    visibilitySeconds,
                    CancellationToken.None);

                logger.LogTrace(
                    "InterBus AmazonSqs: extended visibility timeout for message (receipt={ShortHandle}) by {Seconds}s",
                    receiptHandle[..Math.Min(8, receiptHandle.Length)],
                    visibilitySeconds);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus AmazonSqs: failed to extend visibility timeout for message (receipt={ShortHandle})",
                    receiptHandle[..Math.Min(8, receiptHandle.Length)]);
            }
        }
    }

    private async Task DispatchAsync(ConsumerDescriptor descriptor, Message message, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();

        logger.LogTrace(
            "InterBus AmazonSqs: deserialising message {MessageId}; raw body (first 500 chars): {Body}",
            message.MessageId,
            message.Body?.Length > 500 ? message.Body[..500] + "..." : message.Body);

        if (message.Body is null)
            throw new InvalidOperationException(
                $"SQS message {message.MessageId} has a null body and cannot be deserialised.");

        var envelope = scope.ServiceProvider.GetKeyedService<ISnsSqsMessageEnvelope>(descriptor.ConsumerType)
                       ?? scope.ServiceProvider.GetRequiredService<ISnsSqsMessageEnvelope>();
        var (payload, headers) = envelope.DeserializeFromSqs(message.Body);

        logger.LogTrace(
            "InterBus AmazonSqs: dispatching message {MessageId} with {HeaderCount} headers to consumer {ConsumerType}",
            message.MessageId,
            headers.Count,
            descriptor.ConsumerType.Name);

        var context = new DispatchContext(descriptor, payload, headers);
        await executor.ExecuteAsync(context, ct);
    }
}
