using InterBus.Transport;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace InterBus.RabbitMQ;

/// <summary>
/// RabbitMQ (AMQP) backed <see cref="ITransport"/>.
/// <para>
/// When <see cref="SendAsync"/> is called with a <c>rabbitmqtopic://</c> destination URI,
/// it publishes to a <c>fanout</c> exchange named
/// <c>{<see cref="RabbitMqTransportOptions.ExchangePrefix"/>}{topicName}</c>,
/// delivering to every queue bound to that exchange.
/// </para>
/// <para>
/// When called with a <c>rabbitmqqueue://</c> destination URI, it publishes directly to the
/// named queue using the default exchange (routing key = queue name).
/// </para>
/// <para>
/// Registered as a <em>scoped</em> service; each DI scope obtains its own instance with a
/// dedicated publish channel. Within a single scope operations are sequential so no locking
/// is needed for the channel. When resolved from a singleton context (e.g. via the default
/// singleton <see cref="ITransportFactory"/>), the instance lives for the application
/// lifetime.
/// </para>
/// <para>
/// Publisher confirms are enabled on the channel so that every publish call waits for a broker
/// acknowledgement before returning, preventing silent message loss.
/// </para>
/// <para>
/// Topology declaration and tracking is handled by <see cref="RabbitMqTopologyBuilder"/>, including
/// lazy exchange/queue creation on send paths and startup bindings for consumer queues.
/// </para>
/// <para>
/// Delayed delivery is not natively supported; the <see cref="TransportSendContext.Delay"/>
/// value is ignored and messages are always delivered immediately.
/// </para>
/// </summary>
public sealed class RabbitMqTransport : ITransport, IAsyncDisposable
{
    /// <summary>URI scheme for RabbitMQ topic (fan-out) destinations.</summary>
    public const string TopicScheme = "rabbitmqtopic";

    /// <summary>URI scheme for RabbitMQ queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "rabbitmqqueue";

    private readonly RabbitMqConnectionProvider _connectionProvider;
    private readonly ITopologyBuilder _topologyBuilder;
    private readonly ILogger<RabbitMqTransport> _logger;

    // Channel is created lazily on first SendAsync within the scope.
    private IChannel? _publishChannel;

    internal RabbitMqTransport(
        RabbitMqConnectionProvider connectionProvider,
        ITopologyBuilder topologyBuilder,
        ILogger<RabbitMqTransport> logger)
    {
        _connectionProvider = connectionProvider;
        _topologyBuilder = topologyBuilder;
        _logger = logger;
    }

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        // RabbitMQ does not natively support deferred visibility; deliver immediately.
        var channel = await EnsureChannelAsync(cancellationToken);
        var name = RoutingConventions.GetDestinationName(context.Destination);
        var headers = BuildAmqpHeaders(context.Headers);
        var props = new BasicProperties
        {
            DeliveryMode = DeliveryModes.Persistent,
            Headers = headers,
        };

        if (context.Destination.Scheme == TopicScheme)
        {
            var exchangeName = await _topologyBuilder.EnsureTopicAsync(name, cancellationToken);
            await PublishToExchangeAsync(channel, name, exchangeName, props, context.Payload, cancellationToken);
        }
        else if (context.Destination.Scheme == QueueScheme)
        {
            await _topologyBuilder.EnsureQueueAsync(name, cancellationToken);
            await PublishToQueueAsync(channel, name, props, context.Payload, cancellationToken);
        }
        else
        {
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{TopicScheme}' or '{QueueScheme}'.",
                nameof(context));
        }
    }

    private async Task PublishToExchangeAsync(
        IChannel channel,
        string rawTopicName,
        string exchangeName,
        BasicProperties props,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        try
        {
            await channel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: props,
                body: payload,
                cancellationToken: ct);
        }
        catch (Exception) when (!channel.IsOpen)
        {
            if (_topologyBuilder is RabbitMqTopologyBuilder rabbitMqTopologyBuilder)
                rabbitMqTopologyBuilder.InvalidateTopic(exchangeName);

            _publishChannel = null;
            var freshChannel = await EnsureChannelAsync(ct);
            exchangeName = await _topologyBuilder.EnsureTopicAsync(rawTopicName, ct);
            await freshChannel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: props,
                body: payload,
                cancellationToken: ct);
        }
    }

    private async Task PublishToQueueAsync(
        IChannel channel,
        string queueName,
        BasicProperties props,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        try
        {
            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: false,
                basicProperties: props,
                body: payload,
                cancellationToken: ct);
        }
        catch (Exception) when (!channel.IsOpen)
        {
            if (_topologyBuilder is RabbitMqTopologyBuilder rabbitMqTopologyBuilder)
                rabbitMqTopologyBuilder.InvalidateQueue(queueName);

            _publishChannel = null;
            var freshChannel = await EnsureChannelAsync(ct);
            await _topologyBuilder.EnsureQueueAsync(queueName, ct);
            await freshChannel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: false,
                basicProperties: props,
                body: payload,
                cancellationToken: ct);
        }
    }

    private async Task<IChannel> EnsureChannelAsync(CancellationToken cancellationToken)
    {
        if (_publishChannel is { IsOpen: true })
            return _publishChannel;

        var connection = await _connectionProvider.GetPublishConnectionAsync(cancellationToken);
        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        _publishChannel = await connection.CreateChannelAsync(channelOptions, cancellationToken);

        _logger.LogDebug("InterBus RabbitMQ: publish channel established");

        return _publishChannel;
    }

    private static Dictionary<string, object?> BuildAmqpHeaders(MessageHeaders headers)
    {
        var result = new Dictionary<string, object?>(headers.Count, StringComparer.Ordinal);
        foreach (var kv in headers)
            result[kv.Key] = kv.Value;
        return result;
    }

    internal static MessageHeaders ParseAmqpHeaders(IDictionary<string, object?>? amqpHeaders)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);

        if (amqpHeaders is null) return new MessageHeaders(dict);

        foreach (var kv in amqpHeaders)
        {
            var value = kv.Value switch
            {
                byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
                string str => str,
                _ => kv.Value?.ToString(),
            };
            if (value is not null)
                dict[kv.Key] = value;
        }

        return new MessageHeaders(dict);
    }

    /// <inheritdoc/>
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
