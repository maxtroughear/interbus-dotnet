using System.Web;
using InterBus.Transporting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace InterBus.RabbitMQ;

internal sealed class RabbitMqQueueTransport(
    RabbitMqConnectionProvider connectionProvider,
    ILogger<RabbitMqQueueTransport> logger)
    : ITransport, IAsyncDisposable
{
    public const string QueueScheme = "rabbitmqqueue";

    private IChannel? _publishChannel;

    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var queueName = RoutingConventions.GetDestinationName(context.Destination);

        // If the destination URI carries a virtual queue ID (e.g. ?vq=abc123), inject it as
        // a header so RabbitMqVirtualQueueHost can route the reply to the right in-memory channel.
        var query = HttpUtility.ParseQueryString(context.Destination.Query);
        var virtualQueue = query["vq"];
        if (virtualQueue is not null)
            context.Headers[RabbitMqVirtualQueueHost.VirtualQueueIdHeader] = virtualQueue;

        var headers = RabbitMqHeaders.BuildAmqpHeaders(context.Headers);
        var props = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent,
            Headers = headers,
        };

        for (var i = 0; i < 2; i++)
        {
            var channel = await EnsureChannelAsync(cancellationToken);
            try
            {
                await channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: queueName,
                    mandatory: false,
                    basicProperties: props,
                    body: context.Payload,
                    cancellationToken: cancellationToken);

                return;
            }
            catch (Exception) when (!channel.IsOpen)
            {
                logger.LogWarning("InterBus RabbitMQ: channel closed during publish to {Queue}", queueName);
                _publishChannel = null;
            }
        }
    }

    private async Task<IChannel> EnsureChannelAsync(CancellationToken cancellationToken)
    {
        if (_publishChannel is { IsOpen: true })
            return _publishChannel;

        var connection = await connectionProvider.GetPublishConnectionAsync(cancellationToken);
        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        _publishChannel = await connection.CreateChannelAsync(channelOptions, cancellationToken);

        logger.LogDebug("InterBus RabbitMQ: queue publish channel established");
        return _publishChannel;
    }

    public async ValueTask DisposeAsync()
    {
        if (_publishChannel is not null)
        {
            await _publishChannel.CloseAsync();
            await _publishChannel.DisposeAsync();
            _publishChannel = null;
        }
    }
}
