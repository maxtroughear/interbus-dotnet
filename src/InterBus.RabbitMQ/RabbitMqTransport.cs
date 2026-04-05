using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace InterBus.RabbitMQ;

/// <summary>
/// RabbitMQ (AMQP) backed <see cref="IInterBusTransport"/>.
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
/// singleton <see cref="IInterBusTransportFactory"/>), the instance lives for the application
/// lifetime.
/// </para>
/// <para>
/// Publisher confirms are enabled on the channel so that every
/// <see cref="BasicPublishAsync"/> call waits for a broker acknowledgement before returning,
/// preventing silent message loss.
/// </para>
/// <para>
/// Topology (exchanges and queues) is declared at most once per application lifetime, tracked
/// by the singleton <see cref="RabbitMqConnectionProvider.DeclaredTopology"/> dictionary so
/// that the cost is not repeated across DI scopes. If a publish fails because the channel is
/// closed by the broker (e.g., an exchange or queue was deleted by an operator), the tracking
/// entry is removed, the channel is reopened, topology is redeclared, and the publish is
/// retried once.
/// </para>
/// <para>
/// Delayed delivery is not natively supported; the <see cref="TransportSendContext.Delay"/>
/// value is ignored and messages are always delivered immediately.
/// </para>
/// </summary>
public sealed class RabbitMqTransport : IInterBusTransport, IAsyncDisposable
{
    /// <summary>URI scheme for RabbitMQ topic (fan-out) destinations.</summary>
    public const string TopicScheme = "rabbitmqtopic";

    /// <summary>URI scheme for RabbitMQ queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "rabbitmqqueue";

    private readonly RabbitMqConnectionProvider _connectionProvider;
    private readonly RabbitMqTransportOptions _options;
    private readonly ILogger<RabbitMqTransport> _logger;

    // Channel is created lazily on first SendAsync within the scope.
    private IChannel? _publishChannel;

    internal RabbitMqTransport(
        RabbitMqConnectionProvider connectionProvider,
        RabbitMqTransportOptions options,
        ILogger<RabbitMqTransport> logger)
    {
        _connectionProvider = connectionProvider;
        _options = options;
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
            var exchangeName = _options.ExchangePrefix + name;
            await PublishToExchangeAsync(channel, exchangeName, props, context.Payload, cancellationToken);
        }
        else if (context.Destination.Scheme == QueueScheme)
        {
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

    /// <summary>
    /// Declares the exchange at most once per application lifetime (tracked in the singleton
    /// <see cref="RabbitMqConnectionProvider.DeclaredTopology"/>), then publishes the message.
    /// If the channel is closed by the broker (e.g., the exchange was deleted), removes the
    /// tracking entry, reopens the channel, redeclares, and retries the publish once.
    /// </summary>
    private async Task PublishToExchangeAsync(
        IChannel channel,
        string exchangeName,
        BasicProperties props,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        await EnsureExchangeDeclaredAsync(channel, exchangeName, ct);
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
            // Channel was closed by the broker (exchange may have been deleted).
            // Clear the topology entry, recreate the channel, redeclare, and retry once.
            _connectionProvider.DeclaredTopology.TryRemove(exchangeName, out _);
            _publishChannel = null;
            var freshChannel = await EnsureChannelAsync(ct);
            await EnsureExchangeDeclaredAsync(freshChannel, exchangeName, ct);
            await freshChannel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: props,
                body: payload,
                cancellationToken: ct);
        }
    }

    /// <summary>
    /// Declares the queue at most once per application lifetime (tracked in the singleton
    /// <see cref="RabbitMqConnectionProvider.DeclaredTopology"/>), then publishes the message.
    /// If the channel is closed by the broker (e.g., the queue was deleted), removes the
    /// tracking entry, reopens the channel, redeclares, and retries the publish once.
    /// </summary>
    private async Task PublishToQueueAsync(
        IChannel channel,
        string queueName,
        BasicProperties props,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        await EnsureQueueDeclaredAsync(channel, queueName, ct);
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
            // Channel was closed by the broker (queue may have been deleted).
            // Clear the topology entry, recreate the channel, redeclare, and retry once.
            _connectionProvider.DeclaredTopology.TryRemove(queueName, out _);
            _publishChannel = null;
            var freshChannel = await EnsureChannelAsync(ct);
            await EnsureQueueDeclaredAsync(freshChannel, queueName, ct);
            await freshChannel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queueName,
                mandatory: false,
                basicProperties: props,
                body: payload,
                cancellationToken: ct);
        }
    }

    /// <summary>
    /// Declares <paramref name="exchangeName"/> on the broker if not already recorded in the
    /// application-lifetime <see cref="RabbitMqConnectionProvider.DeclaredTopology"/> set.
    /// Idempotent: safe to call even if the exchange already exists on the broker.
    /// </summary>
    private async Task EnsureExchangeDeclaredAsync(IChannel channel, string exchangeName, CancellationToken ct)
    {
        if (!_connectionProvider.DeclaredTopology.TryAdd(exchangeName, true))
            return;

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: ct);
    }

    /// <summary>
    /// Declares <paramref name="queueName"/> on the broker if not already recorded in the
    /// application-lifetime <see cref="RabbitMqConnectionProvider.DeclaredTopology"/> set.
    /// Idempotent: safe to call even if the queue already exists on the broker.
    /// </summary>
    private async Task EnsureQueueDeclaredAsync(IChannel channel, string queueName, CancellationToken ct)
    {
        if (!_connectionProvider.DeclaredTopology.TryAdd(queueName, true))
            return;

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: ct);
    }

    /// <summary>
    /// Returns the publish channel, creating it on first call within this scope.
    /// No locking is needed because each scope is used sequentially.
    /// Publisher confirms are enabled so every <see cref="BasicPublishAsync"/> waits for
    /// a broker acknowledgement.
    /// </summary>
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

    internal static Dictionary<string, object?> BuildAmqpHeaders(MessageHeaders headers)
    {
        var result = new Dictionary<string, object?>(headers.Count, StringComparer.Ordinal);
        foreach (var kv in headers)
            result[kv.Key] = kv.Value;
        return result;
    }

    internal static MessageHeaders ParseAmqpHeaders(IDictionary<string, object?>? amqpHeaders)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        if (amqpHeaders is not null)
        {
            foreach (var kv in amqpHeaders)
            {
                string? value = kv.Value switch
                {
                    byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
                    string str => str,
                    _ => kv.Value?.ToString(),
                };
                if (value is not null)
                    dict[kv.Key] = value;
            }
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
