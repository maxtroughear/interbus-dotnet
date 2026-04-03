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
/// </summary>
public sealed class RabbitMqTransport : IInterBusTransport, IAsyncDisposable
{
    /// <summary>URI scheme for RabbitMQ topic (fan-out) destinations.</summary>
    public const string TopicScheme = "rabbitmqtopic";

    /// <summary>URI scheme for RabbitMQ queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "rabbitmqqueue";

    private readonly RabbitMqTransportOptions _options;
    private readonly ILogger<RabbitMqTransport> _logger;
    private IConnection? _connection;
    private IChannel? _publishChannel;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _disposed;

    public RabbitMqTransport(RabbitMqTransportOptions options, ILogger<RabbitMqTransport> logger)
    {
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        if (context.Delay.HasValue)
        {
            // Schedule delivery in the background so the caller is not blocked for the delay duration.
            _ = DeliverAfterDelayAsync(context, cancellationToken);
            return;
        }

        await DeliverCoreAsync(context, cancellationToken);
    }

    /// <summary>Waits for the configured delay then forwards the message to the destination.</summary>
    private async Task DeliverAfterDelayAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(context.Delay!.Value, cancellationToken);
            await DeliverCoreAsync(context, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Delivery cancelled (e.g. host shutting down). Silently discard.
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "InterBus RabbitMQ: unhandled error delivering delayed message to {Destination}",
                context.Destination);
        }
    }

    private async Task DeliverCoreAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var channel = await GetPublishChannelAsync(cancellationToken);
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
            await channel.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

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
            await channel.QueueDeclareAsync(
                queue: name,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

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
    /// Returns the shared publish channel, creating the connection and channel on first use.
    /// </summary>
    private async Task<IChannel> GetPublishChannelAsync(CancellationToken cancellationToken)
    {
        if (_publishChannel is { IsOpen: true })
            return _publishChannel;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_publishChannel is { IsOpen: true })
                return _publishChannel;

            if (_connection is not null)
                await _connection.DisposeAsync();
            _connection = await CreateConnectionAsync(cancellationToken);
            _publishChannel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

            _logger.LogDebug("InterBus RabbitMQ: publish connection and channel established");

            return _publishChannel;
        }
        finally
        {
            _initLock.Release();
        }
    }

    internal async Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var factory = BuildConnectionFactory();
        return await factory.CreateConnectionAsync(cancellationToken);
    }

    internal ConnectionFactory BuildConnectionFactory()
    {
        var factory = new ConnectionFactory();

        if (!string.IsNullOrWhiteSpace(_options.Uri))
        {
            factory.Uri = new Uri(_options.Uri);
        }
        else
        {
            factory.HostName = _options.HostName;
            factory.Port = _options.Port;
            factory.VirtualHost = _options.VirtualHost;
            factory.UserName = _options.UserName;
            factory.Password = _options.Password;
        }

        return factory;
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
        if (_disposed) return;
        _disposed = true;

        if (_publishChannel is not null)
            await _publishChannel.CloseAsync();

        if (_connection is not null)
            await _connection.CloseAsync();

        _initLock.Dispose();
    }
}
