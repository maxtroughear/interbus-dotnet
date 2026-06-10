using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SQS implementation of <see cref="IReplyEndpoint"/>.
/// Polls a temporary SQS queue for a single reply message, dispatches it through the full
/// consume pipeline, then deletes the queue on disposal.
/// </summary>
internal sealed class SqsReplyEndpoint(
    IAmazonSQS sqsClient,
    SnsSqsTopology topology,
    ISnsSqsMessageEnvelope envelope,
    IServiceScopeFactory scopeFactory,
    ILogger<SqsReplyEndpoint> logger,
    string queueName,
    string queueUrl)
    : IReplyEndpoint
{
    public Uri Address { get; } = new Uri($"{SqsTransport.QueueScheme}://{queueName}");

    public async Task<TReply> ReceiveAsync<TReply>(CancellationToken cancellationToken = default)
        where TReply : class
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            List<Message> messages;
            try
            {
                var response = await sqsClient.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MaxNumberOfMessages = 1,
                        WaitTimeSeconds = 20,
                        MessageAttributeNames = ["All"],
                    },
                    cancellationToken);

                messages = response.Messages ?? [];
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            foreach (var message in messages)
            {
                if (message.Body is null)
                    continue;

                var (payload, headers) = envelope.DeserializeFromSqs(message.Body);

                var tcs = new TaskCompletionSource<TReply>(TaskCreationOptions.RunContinuationsAsynchronously);

                var descriptor = ConsumerDescriptor.CreateForReply<TReply>(
                    Address,
                    reply =>
                    {
                        tcs.TrySetResult(reply);
                        return Task.CompletedTask;
                    });

                try
                {
                    await using var scope = scopeFactory.CreateAsyncScope();
                    var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
                    var context = new DispatchContext(descriptor, payload, headers);
                    await executor.ExecuteAsync(context, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }

                if (tcs.Task.IsCompleted)
                {
                    // Use CancellationToken.None so that a graceful shutdown does not leave a
                    // successfully-dispatched reply to be redelivered.
                    await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None);
                    return await tcs.Task;
                }

                // Wrong message type — delete and keep polling.
                logger.LogTrace(
                    "InterBus AmazonSqs: reply endpoint skipping unexpected message type; expected {ExpectedType}",
                    typeof(TReply).Name);

                // Use CancellationToken.None so that an unexpected message is always deleted
                // rather than left to time out and be redelivered.
                await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new InvalidOperationException(
            $"InterBus AmazonSqs: reply channel closed without receiving a {typeof(TReply).Name}");
    }

    public async ValueTask DisposeAsync()
    {
        topology.QueueUrls.TryRemove(queueName, out _);

        try
        {
            await sqsClient.DeleteQueueAsync(queueUrl, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "InterBus AmazonSqs: failed to delete temporary reply queue {QueueName}",
                queueName);
        }
    }
}
