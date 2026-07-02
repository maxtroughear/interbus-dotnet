using System.Collections.Concurrent;
using System.Threading.Channels;
using InterBus.Requests;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace InterBus.RabbitMQ;

/// <summary>
/// Singleton that manages a single shared exclusive RabbitMQ queue used for all request/reply
/// exchanges in the current application instance.
/// <para>
/// Each call to <see cref="RegisterAsync"/> generates a unique virtual queue ID and allocates an
/// in-memory <see cref="Channel{T}"/> for that exchange. A background AMQP consumer reads
/// messages from the shared queue and routes each reply to the correct channel using the
/// <c>interbus-rabbitmq-vq-id</c> header.
/// </para>
/// <para>
/// The RabbitMQ queue is declared as exclusive and auto-delete so it is automatically removed
/// when the publish connection closes (application shutdown). This avoids per-request queue
/// create/delete latency while ensuring resources are cleaned up without manual coordination.
/// </para>
/// </summary>
internal sealed class RabbitMqVirtualQueueHost(
    RabbitMqConnectionProvider connectionProvider,
    ILogger<RabbitMqVirtualQueueHost> logger)
    : IVirtualQueueHost, IAsyncDisposable
{
    /// <summary>Header key used to route a reply to the correct virtual queue.</summary>
    internal const string VirtualQueueIdHeader = "interbus-rabbitmq-vq-id";

    private readonly SemaphoreSlim _initLock = new(1, 1);

    private readonly ConcurrentDictionary<string, Channel<(byte[] Payload, MessageHeaders Headers)>> _channels =
        new(StringComparer.Ordinal);

    private string? _queueName;
    private IChannel? _consumeChannel;

    /// <summary>
    /// Lazily creates the shared reply queue (once) then allocates a new in-memory channel keyed
    /// by a freshly generated virtual queue ID.
    /// </summary>
    /// <returns>
    /// The raw reply queue name, the new virtual queue ID, and a reader end of the channel that
    /// will receive exactly one reply payload.
    /// </returns>
    public async Task<VirtualQueueRegistration> RegisterAsync(CancellationToken cancellationToken = default)
    {
        await EnsureHostQueueAsync(cancellationToken);

        var vqId = Guid.CreateVersion7().ToString("N");
        var channel = Channel.CreateUnbounded<(byte[], MessageHeaders)>(
            new UnboundedChannelOptions { SingleReader = true });
        _channels[vqId] = channel;

        return new VirtualQueueRegistration(_queueName!, vqId, channel.Reader);
    }

    /// <summary>Completes and removes the channel for <paramref name="vqId"/>.</summary>
    public void Unregister(string vqId)
    {
        if (_channels.TryRemove(vqId, out var channel))
            channel.Writer.TryComplete();
    }

    private async Task EnsureHostQueueAsync(CancellationToken cancellationToken)
    {
        if (_queueName is not null)
            return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_queueName is not null)
                return;

            var connection = await connectionProvider.GetPublishConnectionAsync(cancellationToken);
            _consumeChannel = await connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: false,
                    publisherConfirmationTrackingEnabled: false),
                cancellationToken);

            // Declare an exclusive, auto-delete queue: tied to this connection, no manual cleanup needed.
            var result = await _consumeChannel.QueueDeclareAsync(
                queue: string.Empty,
                durable: false,
                exclusive: true,
                autoDelete: true,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            _queueName = result.QueueName;

            var consumer = new AsyncEventingBasicConsumer(_consumeChannel);
            consumer.ReceivedAsync += HandleReplyAsync;

            await _consumeChannel.BasicConsumeAsync(
                queue: _queueName,
                autoAck: true,
                consumer: consumer,
                cancellationToken: cancellationToken);

            logger.LogDebug("InterBus RabbitMQ: created virtual queue host {QueueName}", _queueName);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private Task HandleReplyAsync(object sender, BasicDeliverEventArgs ea)
    {
        try
        {
            var headers = RabbitMqHeaders.ParseAmqpHeaders(ea.BasicProperties.Headers);

            if (!headers.TryGetValue(VirtualQueueIdHeader, out var vqId) ||
                !_channels.TryGetValue(vqId, out var channel))
            {
                logger.LogWarning(
                    "InterBus RabbitMQ: received reply on virtual queue host with unknown or missing vq-id; discarding");
                return Task.CompletedTask;
            }

            channel.Writer.TryWrite((ea.Body.ToArray(), headers));
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "InterBus RabbitMQ: failed to route reply message on virtual queue host; discarding");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        foreach (var channel in _channels.Values)
            channel.Writer.TryComplete();
        _channels.Clear();

        if (_consumeChannel is not null)
        {
            try
            {
                await _consumeChannel.CloseAsync(CancellationToken.None);
                await _consumeChannel.DisposeAsync();
            }
            catch
            {
                // Ignored on disposal.
            }

            _consumeChannel = null;
        }

        _initLock.Dispose();
    }
}
