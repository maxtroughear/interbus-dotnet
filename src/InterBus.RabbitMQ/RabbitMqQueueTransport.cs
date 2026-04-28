using InterBus.Transporting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace InterBus.RabbitMQ;

internal sealed class RabbitMqQueueTransport(
    RabbitMqConnectionProvider connectionProvider,
    ITopologyBuilder topologyBuilder,
    ILogger<RabbitMqQueueTransport> logger)
    : ITransport, IAsyncDisposable
{
    public const string QueueScheme = "rabbitmqqueue";

    private IChannel? _publishChannel;

    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var channel = await EnsureChannelAsync(cancellationToken);
        var queueName = RoutingConventions.GetDestinationName(context.Destination);
        var headers = RabbitMqHeaders.BuildAmqpHeaders(context.Headers);
        var props = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent,
            Headers = headers,
        };

        await topologyBuilder.EnsureQueueAsync(queueName, cancellationToken);

        try
        {
            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: false,
                basicProperties: props,
                body: context.Payload,
                cancellationToken: cancellationToken);
        }
        catch (Exception) when (!channel.IsOpen)
        {
            if (topologyBuilder is RabbitMqTopologyBuilder rabbitMqTopologyBuilder)
                rabbitMqTopologyBuilder.InvalidateQueue(queueName);

            _publishChannel = null;
            var freshChannel = await EnsureChannelAsync(cancellationToken);
            await topologyBuilder.EnsureQueueAsync(queueName, cancellationToken);
            await freshChannel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: false,
                basicProperties: props,
                body: context.Payload,
                cancellationToken: cancellationToken);
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
