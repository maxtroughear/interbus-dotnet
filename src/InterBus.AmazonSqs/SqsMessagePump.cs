using System.Text.Json;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that declares the
/// required SNS/SQS topology (topics, queues, subscriptions) at startup and starts one
/// long-polling consumer per registered InterBus consumer queue.
/// <para>
/// Each consumer queue is bound via an SNS subscription to every SNS topic that corresponds
/// to a message type the consumer handles. Direct sends (<c>sqsqueue://</c>) arrive without
/// an SNS wrapper.
/// </para>
/// <para>
/// Messages delivered via SNS use <em>raw message delivery</em> so the SQS message body is
/// the original payload and SNS message attributes are promoted to SQS message attributes.
/// This keeps parsing uniform for both SNS-fanned and direct SQS messages.
/// </para>
/// <para>
/// Successful dispatch deletes the SQS message (equivalent of ACK). Consumer failures let
/// the message's visibility timeout expire so it is redelivered by SQS automatically.
/// </para>
/// </summary>
internal sealed class SqsMessagePump(
    IAmazonSimpleNotificationService snsClient,
    IAmazonSQS sqsClient,
    SnsSqsClientProvider clientProvider,
    SnsSqsTransportOptions options,
    InterBusOptions busOptions,
    IServiceScopeFactory scopeFactory,
    ILogger<SqsMessagePump> logger)
    : BackgroundService
{
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (busOptions.Consumers.Count > 0)
        {
            await SetupTopologyWithRetryAsync(cancellationToken);
        }

        await base.StartAsync(cancellationToken);
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

    private async Task SetupTopologyWithRetryAsync(CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                await DeclareTopologyAsync(cancellationToken);
                return;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                attempt++;
                if (attempt > options.ConnectionRetry.RetryCount)
                    throw;

                var delay = options.ConnectionRetry.DelayProvider(attempt);
                logger.LogWarning(
                    ex,
                    "InterBus AmazonSqs: topology setup failed (attempt {Attempt}); retrying in {Delay}",
                    attempt,
                    delay);

                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Creates the SQS queue for each registered consumer and, for each message type the
    /// consumer handles, creates the corresponding SNS topic and subscribes the queue to
    /// that topic using raw message delivery.
    /// </summary>
    private async Task DeclareTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = clientProvider.BuildSqsQueueName(descriptor.QueueName);

            logger.LogDebug(
                "InterBus AmazonSqs: declaring queue {QueueName} for consumer {ConsumerType}",
                queueName,
                descriptor.ConsumerType.Name);

            var queueResponse = await sqsClient.CreateQueueAsync(
                new CreateQueueRequest
                {
                    QueueName = queueName,
                    Attributes = new Dictionary<string, string>
                    {
                        ["MessageRetentionPeriod"] = "1209600", // 14 days
                    },
                },
                cancellationToken);

            var queueUrl = queueResponse.QueueUrl;
            clientProvider.QueueUrls.TryAdd(queueName, queueUrl);

            // Retrieve the queue ARN needed for the SQS resource policy.
            var attrs = await sqsClient.GetQueueAttributesAsync(
                new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = ["QueueArn"],
                },
                cancellationToken);

            var queueArn = attrs.QueueARN;

            // Subscribe the queue to each SNS topic the consumer handles.
            foreach (var messageType in descriptor.MessageTypes)
            {
                var rawTopicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var topicName = clientProvider.BuildSnsTopicName(rawTopicName);

                logger.LogDebug(
                    "InterBus AmazonSqs: declaring SNS topic {TopicName}",
                    topicName);

                var topicResponse = await snsClient.CreateTopicAsync(
                    topicName,
                    cancellationToken);

                var topicArn = topicResponse.TopicArn;
                clientProvider.TopicArns.TryAdd(topicName, topicArn);

                // Subscribe queue to topic.
                var subscribeResponse = await snsClient.SubscribeAsync(
                    new SubscribeRequest
                    {
                        TopicArn = topicArn,
                        Protocol = "sqs",
                        Endpoint = queueArn,
                        Attributes = new Dictionary<string, string>
                        {
                            // Raw message delivery passes the original payload and
                            // message attributes directly to SQS without wrapping in
                            // an SNS notification JSON envelope.
                            ["RawMessageDelivery"] = "true",
                        },
                    },
                    cancellationToken);

                logger.LogDebug(
                    "InterBus AmazonSqs: subscribed queue {QueueName} to topic {TopicName} (subscriptionArn={SubscriptionArn})",
                    queueName,
                    topicName,
                    subscribeResponse.SubscriptionArn);
            }

            // Allow SNS to send messages to this SQS queue.
            await SetQueuePolicyAsync(queueUrl, queueArn, descriptor, cancellationToken);
        }
    }

    /// <summary>
    /// Attaches an SQS resource-based policy that allows all SNS topics associated with
    /// this consumer's message types to send messages to the queue.
    /// </summary>
    private async Task SetQueuePolicyAsync(
        string queueUrl,
        string queueArn,
        ConsumerDescriptor descriptor,
        CancellationToken cancellationToken)
    {
        var topicArns = descriptor.MessageTypes.Select(t =>
        {
            var rawName = busOptions.TryGetTopicName(t, out var custom)
                ? custom
                : RoutingConventions.TopicName(t);

            if (clientProvider.TopicArns.TryGetValue(
                    clientProvider.BuildSnsTopicName(rawName),
                    out var arn))
            {
                return arn;
            }

            return null;
        }).OfType<string>().Distinct().ToList();

        if (topicArns.Count == 0)
            return;

        var arnList = topicArns.Count == 1
            ? $"\"{topicArns[0]}\""
            : "[" + string.Join(",", topicArns.Select(a => $"\"{a}\"")) + "]";

        var conditionBlock =
            "{\"StringEquals\":{\"aws:SourceArn\":" + arnList + "}}";

        var policy =
            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\"," +
            "\"Principal\":{\"Service\":\"sns.amazonaws.com\"}," +
            "\"Action\":\"sqs:SendMessage\"," +
            $"\"Resource\":\"{queueArn}\"," +
            $"\"Condition\":{conditionBlock}" +
            "}]}";

        await sqsClient.SetQueueAttributesAsync(
            new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string> { ["Policy"] = policy },
            },
            cancellationToken);
    }

    private async Task ConsumeQueueAsync(ConsumerDescriptor descriptor, CancellationToken stoppingToken)
    {
        var queueName = clientProvider.BuildSqsQueueName(descriptor.QueueName);

        if (!clientProvider.QueueUrls.TryGetValue(queueName, out var queueUrl))
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
        var activeTasks = new List<Task>();

        while (!stoppingToken.IsCancellationRequested)
        {
            // Determine how many messages to fetch based on current processing capacity.
            // Use the semaphore's available count so we don't over-fetch beyond what we
            // can immediately process, plus any optional prefetch buffer.
            var availableSlots = semaphore.CurrentCount;
            var maxFetch = Math.Clamp(availableSlots + options.PrefetchCount, 1, 10);

            // When all slots are busy and prefetch is disabled, wait briefly to avoid
            // a tight poll loop before trying again.
            if (availableSlots == 0 && options.PrefetchCount == 0)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                continue;
            }

            List<Message> messages;
            try
            {
                var response = await sqsClient.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MaxNumberOfMessages = maxFetch,
                        WaitTimeSeconds = 20, // long-poll
                        MessageAttributeNames = ["All"],
                        // Set the visibility timeout explicitly so the pump's renewal
                        // timer knows how long to extend each cycle.
                        VisibilityTimeout = options.VisibilityTimeoutSeconds,
                    },
                    stoppingToken);

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

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);
                continue;
            }

            foreach (var message in messages)
            {
                await semaphore.WaitAsync(stoppingToken);

                // Capture for the lambda below.
                var captured = message;
                var task = Task.Run(
                    async () =>
                    {
                        try
                        {
                            await ProcessMessageAsync(descriptor, queueUrl, captured, stoppingToken);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    },
                    CancellationToken.None);

                activeTasks.Add(task);

                // Remove completed tasks to prevent unbounded growth.
                activeTasks.RemoveAll(t => t.IsCompleted);
            }
        }

        // Wait for all in-flight processing tasks to complete before declaring the pump stopped.
        // This ensures that messages that were successfully dispatched are also deleted from SQS,
        // preventing unnecessary redelivery.
        if (activeTasks.Count > 0)
        {
            try
            {
                await Task.WhenAll(activeTasks);
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
        Message message,
        CancellationToken ct)
    {
        // Start a background renewal task that extends the message's visibility timeout
        // at half-interval cycles so SQS does not redeliver while the message is in
        // flight through a long-running consumer.
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewalTask = ExtendVisibilityAsync(queueUrl, message.ReceiptHandle, renewalCts.Token);

        try
        {
            await DispatchAsync(descriptor, message, ct);

            // Processing succeeded – stop renewal before deleting.
            await renewalCts.CancelAsync();

            // ACK: delete the message from SQS.
            // Use CancellationToken.None so that a graceful shutdown does not leave
            // successfully-dispatched messages to be redelivered unnecessarily.
            await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle,
                CancellationToken.None);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            await renewalCts.CancelAsync();
            // Dispatch was cancelled mid-flight; do not delete – SQS will redeliver.
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "InterBus AmazonSqs: consumer {ConsumerType} failed for queue {QueueName}; message will be redelivered after visibility timeout",
                descriptor.ConsumerType.Name,
                descriptor.QueueName);

            await renewalCts.CancelAsync();
            // Do not delete – SQS will redeliver after the visibility timeout.
        }
        finally
        {
            try { await renewalTask; } catch { }
        }
    }

    /// <summary>
    /// Periodically extends the visibility timeout of an in-flight SQS message so that
    /// long-running consumers do not cause inadvertent redelivery. Runs until
    /// <paramref name="ct"/> is cancelled (typically just before the message is deleted
    /// or after a processing failure).
    /// </summary>
    private async Task ExtendVisibilityAsync(string queueUrl, string receiptHandle, CancellationToken ct)
    {
        // Extend at half the visibility window so there is always a safety margin.
        var extensionInterval = TimeSpan.FromSeconds(
            Math.Max(1, options.VisibilityTimeoutSeconds / 2));

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
                    options.VisibilityTimeoutSeconds,
                    CancellationToken.None);

                logger.LogTrace(
                    "InterBus AmazonSqs: extended visibility timeout for message (receipt={ShortHandle}) by {Seconds}s",
                    receiptHandle[..Math.Min(8, receiptHandle.Length)],
                    options.VisibilityTimeoutSeconds);
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
        var headers = SqsTransport.ParseSqsMessageAttributes(message.MessageAttributes);
        // Decode the base64-encoded payload written by SnsTransport or SqsTransport.
        var payload = Convert.FromBase64String(message.Body);
        var context = new DispatchContext(descriptor, payload, headers);
        await executor.ExecuteAsync(context, ct);
    }
}
