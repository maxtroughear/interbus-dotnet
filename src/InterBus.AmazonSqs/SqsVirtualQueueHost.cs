using System.Collections.Concurrent;
using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

/// <summary>
/// Singleton that manages a single real SQS "host" queue shared by all request/reply
/// exchanges in the current application instance.
/// <para>
/// Each call to <see cref="RegisterAsync"/> allocates a <em>virtual</em> queue — an
/// in-memory <see cref="Channel{T}"/> identified by a unique ID embedded as the
/// <c>interbus-sqs-vq-id</c> header in the reply message.  A single background loop
/// long-polls the host queue, reads that header, and routes each message to the correct
/// channel.  This avoids the per-request latency and AWS API quota cost of creating and
/// deleting a real SQS queue for every exchange.
/// </para>
/// </summary>
internal sealed class SqsVirtualQueueHost(
    IAmazonSQS sqsClient,
    SnsSqsTopology topology,
    SnsSqsTransportOptions options,
    ISnsSqsMessageEnvelope envelope,
    ILogger<SqsVirtualQueueHost> logger)
    : IAsyncDisposable
{
    /// <summary>Header key used to route a reply to the correct virtual queue.</summary>
    internal const string VirtualQueueIdHeader = "interbus-sqs-vq-id";

    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly ConcurrentDictionary<string, Channel<(byte[] Payload, MessageHeaders Headers)>> _channels =
        new(StringComparer.Ordinal);

    private string? _hostQueueName;
    private string? _hostQueueUrl;
    private CancellationTokenSource? _pollingCts;
    private Task? _pollingTask;

    /// <summary>
    /// Lazily creates the shared host queue (once) then allocates a new virtual queue
    /// and returns its ID together with the address of the host queue.
    /// </summary>
    public async Task<(string HostQueueName, string VqId, ChannelReader<(byte[] Payload, MessageHeaders Headers)> Reader)>
        RegisterAsync(CancellationToken cancellationToken = default)
    {
        await EnsureHostQueueAsync(cancellationToken);

        var vqId = Guid.CreateVersion7().ToString("N");
        var channel = Channel.CreateUnbounded<(byte[], MessageHeaders)>(
            new UnboundedChannelOptions { SingleReader = true });
        _channels[vqId] = channel;

        return (_hostQueueName!, vqId, channel.Reader);
    }

    /// <summary>Completes and removes the channel for <paramref name="vqId"/>.</summary>
    public void Unregister(string vqId)
    {
        if (_channels.TryRemove(vqId, out var channel))
            channel.Writer.TryComplete();
    }

    private async Task EnsureHostQueueAsync(CancellationToken cancellationToken)
    {
        if (_hostQueueUrl is not null)
            return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_hostQueueUrl is not null)
                return;

            var queueName = $"interbus-reply-{Guid.CreateVersion7():N}";

            var attributes = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["MessageRetentionPeriod"] = "300",
            };

            if (options.EnableSqsEncryption)
                attributes["SqsManagedSseEnabled"] = "true";

            var response = await sqsClient.CreateQueueAsync(
                new CreateQueueRequest { QueueName = queueName, Attributes = attributes },
                cancellationToken);

            _hostQueueName = queueName;
            _hostQueueUrl = response.QueueUrl;

            // Register in topology so SqsTransport.ResolveQueueUrlAsync finds this queue.
            topology.QueueUrls.TryAdd(queueName, _hostQueueUrl);

            _pollingCts = new CancellationTokenSource();
            _pollingTask = Task.Run(() => PollLoopAsync(_pollingCts.Token), CancellationToken.None);

            logger.LogDebug(
                "InterBus AmazonSqs: created virtual queue host {QueueName}",
                queueName);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task PollLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            List<Message> messages;
            try
            {
                var response = await sqsClient.ReceiveMessageAsync(
                    new ReceiveMessageRequest
                    {
                        QueueUrl = _hostQueueUrl!,
                        MaxNumberOfMessages = 10,
                        WaitTimeSeconds = 20,
                        MessageAttributeNames = ["All"],
                    },
                    ct);
                messages = response.Messages ?? [];
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus AmazonSqs: error polling virtual queue host {QueueName}; retrying",
                    _hostQueueName);
                try { await Task.Delay(TimeSpan.FromSeconds(5), ct); } catch { }
                continue;
            }

            foreach (var message in messages)
            {
                RouteMessage(message);

                // Always delete: replies are consumed exactly once.
                await sqsClient.DeleteMessageAsync(
                    _hostQueueUrl!,
                    message.ReceiptHandle,
                    CancellationToken.None);
            }
        }
    }

    private void RouteMessage(Message message)
    {
        if (message.Body is null)
            return;

        try
        {
            var (payload, headers) = envelope.DeserializeFromSqs(message.Body);

            if (!headers.TryGetValue(VirtualQueueIdHeader, out var vqId) ||
                !_channels.TryGetValue(vqId, out var channel))
            {
                logger.LogWarning(
                    "InterBus AmazonSqs: received reply on virtual queue host with unknown or missing vq-id; discarding");
                return;
            }

            channel.Writer.TryWrite((payload, headers));
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "InterBus AmazonSqs: failed to deserialise reply message on virtual queue host; discarding");
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_pollingCts is not null)
        {
            await _pollingCts.CancelAsync();
            if (_pollingTask is not null)
            {
                try
                {
                    await _pollingTask;
                }
                catch
                {
                    // ignored
                }
            }

            _pollingCts.Dispose();
        }

        foreach (var channel in _channels.Values)
            channel.Writer.TryComplete();
        _channels.Clear();

        if (_hostQueueUrl is not null)
        {
            topology.QueueUrls.TryRemove(_hostQueueName!, out _);
            try
            {
                await sqsClient.DeleteQueueAsync(_hostQueueUrl!, CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus AmazonSqs: failed to delete virtual queue host {QueueName}",
                    _hostQueueName);
            }
        }

        _initLock.Dispose();
    }
}
