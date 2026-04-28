using InterBus.Transporting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace InterBus.RabbitMQ;

internal sealed class RabbitMqTopicTransport(
    RabbitMqConnectionProvider connectionProvider,
    ITopologyBuilder topologyBuilder,
    ILogger<RabbitMqTopicTransport> logger)
    : ITransport, IAsyncDisposable
{
    public const string TopicScheme = "rabbitmqtopic";

    private IChannel? _publishChannel;

    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var channel = await EnsureChannelAsync(cancellationToken);
        var topicName = RoutingConventions.GetDestinationName(context.Destination);
        var headers = RabbitMqHeaders.BuildAmqpHeaders(context.Headers);
        var props = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent,
            Headers = headers,
        };

        var exchangeName = await topologyBuilder.EnsureTopicAsync(topicName, cancellationToken);

        try
        {
            await channel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: props,
                body: context.Payload,
                cancellationToken: cancellationToken);
        }
        catch (Exception) when (!channel.IsOpen)
        {
            if (topologyBuilder is RabbitMqTopologyBuilder rabbitMqTopologyBuilder)
                rabbitMqTopologyBuilder.InvalidateTopic(exchangeName);

            _publishChannel = null;
            var freshChannel = await EnsureChannelAsync(cancellationToken);
            exchangeName = await topologyBuilder.EnsureTopicAsync(topicName, cancellationToken);
            await freshChannel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: string.Empty,
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

        logger.LogDebug("InterBus RabbitMQ: topic publish channel established");
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
