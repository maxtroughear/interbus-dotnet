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
/// Before the first publish to each exchange or queue within an instance lifetime, the
/// transport performs a single idempotent topology declaration (exchange or queue) as a
/// safety net for publisher-only deployments where no <see cref="RabbitMqMessagePump"/> is
/// running.
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

    // Tracks destinations declared within this scope to avoid redundant declarations.
    private readonly HashSet<string> _declaredDestinations = new(StringComparer.Ordinal);

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

            // Try once to declare the exchange as a safety net for publisher-only deployments.
            if (_declaredDestinations.Add(exchangeName))
            {
                await channel.ExchangeDeclareAsync(
                    exchange: exchangeName,
                    type: ExchangeType.Fanout,
                    durable: true,
                    autoDelete: false,
                    arguments: null,
                    noWait: false,
                    cancellationToken: cancellationToken);
            }

            await channel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: props,
                body: context.Payload,
                cancellationToken: cancellationToken);
        }
        else if (context.Destination.Scheme == QueueScheme)
        {
            // Try once to declare the queue as a safety net for publisher-only deployments.
            if (_declaredDestinations.Add(name))
            {
                await channel.QueueDeclareAsync(
                    queue: name,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    noWait: false,
                    cancellationToken: cancellationToken);
            }

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: name,
                mandatory: false,
                basicProperties: props,
                body: context.Payload,
                cancellationToken: cancellationToken);
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
