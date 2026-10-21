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
    ILogger<SqsQueueMessagePump> logger) : IBufferedSource
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

    public async Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken)
    {
        while (!intakeToken.IsCancellationRequested)
        {
            var reservations = new List<DeliverySink.Reservation> { await sink.ReserveAsync(intakeToken) };
            var deliveries = new List<SqsPrefetchedMessage>();
            try
            {
                while (reservations.Count < 10 && sink.TryReserve() is { } reservation)
                    reservations.Add(reservation);
                ReceiveMessageResponse response;
                try
                {
                    response = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                    {
                        QueueUrl = _queueUrl,
                        MaxNumberOfMessages = reservations.Count,
                        WaitTimeSeconds = options.ReceiveWaitTimeSeconds,
                        VisibilityTimeout = (int)options.VisibilityTimeout.TotalSeconds,
                        MessageAttributeNames = ["All"],
                        MessageSystemAttributeNames = ["MessageGroupId", "MessageDeduplicationId"]
                    }, intakeToken);
                }
                catch (OperationCanceledException) when (intakeToken.IsCancellationRequested) { throw; }
                catch (Exception error)
                {
                    logger.LogWarning(error, "InterBus SQS: receive failed for {Queue}; retrying", _queueUrl);
                    await Task.Delay(TimeSpan.FromSeconds(5), intakeToken);
                    continue;
                }

                // Eager construction starts renewal for the entire batch before any handoff
                foreach (var message in response.Messages ?? [])
                    deliveries.Add(new SqsPrefetchedMessage(loggerFactory.CreateLogger<SqsPrefetchedMessage>(),
                        sqsClient, options, message, _queueUrl, _deadLetterQueueUrl, envelope));
                for (var i = 0; i < deliveries.Count; i++)
                {
                    intakeToken.ThrowIfCancellationRequested();
                    reservations[i].Handoff(deliveries[i]);
                    deliveries[i] = null!;
                }
            }
            finally
            {
                foreach (var delivery in deliveries)
                    if (delivery is not null) await delivery.DisposeAsync();
                foreach (var reservation in reservations) reservation.Dispose();
            }
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
