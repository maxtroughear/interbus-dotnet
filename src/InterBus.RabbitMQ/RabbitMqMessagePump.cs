using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace InterBus.RabbitMQ;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that declares the
/// required RabbitMQ topology (exchanges, queues, bindings) at startup and starts one consumer
/// per registered InterBus consumer queue.
/// <para>
/// Each consumer queue is bound to a <c>fanout</c> exchange for every topic it handles.
/// Messages published to a topic exchange are fanned out to all bound queues. Direct sends
/// (queue-to-queue) use the default exchange with the queue name as the routing key.
/// </para>
/// <para>
/// Successful dispatch acknowledges the RabbitMQ message. Consumer failures issue a negative
/// acknowledgement with <c>requeue=true</c> so the message is redelivered.
/// </para>
/// </summary>
internal sealed class RabbitMqMessagePump : BackgroundService
{
    private readonly RabbitMqTransport _transport;
    private readonly RabbitMqTransportOptions _options;
    private readonly InterBusOptions _busOptions;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<RabbitMqMessagePump> _logger;

    private IConnection? _connection;

    public RabbitMqMessagePump(
        RabbitMqTransport transport,
        RabbitMqTransportOptions options,
        InterBusOptions busOptions,
        IServiceScopeFactory scopeFactory,
        ILogger<RabbitMqMessagePump> logger)
    {
        _transport = transport;
        _options = options;
        _busOptions = busOptions;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    /// <summary>
    /// Opens the consume connection and declares the full RabbitMQ topology before the
    /// background loop starts. By completing topology setup here, publishers can route
    /// messages as soon as <see cref="StartAsync"/> returns without risking a race where
    /// a publish happens before the exchange bindings exist (which would silently drop the message
    /// on a fanout exchange with no bound queues).
    /// </summary>
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_busOptions.Consumers.Count > 0)
        {
            _connection = await _transport.CreateConnectionAsync(cancellationToken);
            _logger.LogDebug("InterBus RabbitMQ: consume connection established");
            await DeclareTopologyAsync(cancellationToken);
        }

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = _busOptions.Consumers;
        if (consumers.Count == 0)
        {
            _logger.LogInformation("InterBus RabbitMQ: no consumers registered; message pump idle");
            return;
        }

        // Start one consumer per queue; all run concurrently.
        var tasks = consumers.Select(d => ConsumeQueueAsync(d, stoppingToken));
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Declares all queues, exchanges, and bindings. Idempotent — safe to call even if the
    /// topology already exists from a previous run. Uses a short-lived channel that is closed
    /// after setup; the per-consumer channels are created later in
    /// <see cref="ConsumeQueueAsync"/>.
    /// </summary>
    private async Task DeclareTopologyAsync(CancellationToken cancellationToken)
    {
        await using var channel = await _connection!.CreateChannelAsync(cancellationToken: cancellationToken);

        foreach (var descriptor in _busOptions.Consumers)
        {
            // Declare the queue as durable so it survives broker restarts.
            await channel.QueueDeclareAsync(
                queue: descriptor.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            // Bind the queue to a fanout exchange for each topic this consumer handles.
            foreach (var messageType in descriptor.MessageTypes)
            {
                var topicName = _busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var exchangeName = _options.ExchangePrefix + topicName;

                await channel.ExchangeDeclareAsync(
                    exchange: exchangeName,
                    type: ExchangeType.Fanout,
                    durable: true,
                    autoDelete: false,
                    arguments: null,
                    noWait: false,
                    cancellationToken: cancellationToken);

                await channel.QueueBindAsync(
                    queue: descriptor.QueueName,
                    exchange: exchangeName,
                    routingKey: string.Empty,
                    arguments: null,
                    noWait: false,
                    cancellationToken: cancellationToken);

                _logger.LogDebug(
                    "InterBus RabbitMQ: bound queue {QueueName} to exchange {ExchangeName}",
                    descriptor.QueueName,
                    exchangeName);
            }
        }
    }

    private async Task ConsumeQueueAsync(ConsumerDescriptor descriptor, CancellationToken stoppingToken)
    {
        _logger.LogDebug(
            "InterBus RabbitMQ: starting consumer for queue {QueueName}",
            descriptor.QueueName);

        var channel = await _connection!.CreateChannelAsync(cancellationToken: stoppingToken);

        // Prefetch one message at a time for fair dispatch.
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            if (stoppingToken.IsCancellationRequested)
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
                return;
            }

            try
            {
                await DispatchAsync(descriptor, ea, stoppingToken);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "InterBus RabbitMQ: consumer {ConsumerType} failed for queue {QueueName}; requeueing message for redelivery",
                    descriptor.ConsumerType.Name,
                    descriptor.QueueName);

                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
            }
        };

        await channel.BasicConsumeAsync(
            queue: descriptor.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        // Block until the stopping token fires, then clean up the channel.
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }

        _logger.LogDebug(
            "InterBus RabbitMQ: consumer for queue {QueueName} stopped",
            descriptor.QueueName);

        await channel.CloseAsync(CancellationToken.None);
        await channel.DisposeAsync();
    }

    private async Task DispatchAsync(ConsumerDescriptor descriptor, BasicDeliverEventArgs ea, CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var executor = scope.ServiceProvider.GetRequiredService<DispatchPipelineExecutor>();
        var headers = RabbitMqTransport.ParseAmqpHeaders(ea.BasicProperties.Headers);
        var context = new DispatchContext(descriptor, ea.Body.ToArray(), headers);
        await executor.ExecuteAsync(context, ct);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        if (_connection is not null)
        {
            await _connection.CloseAsync(cancellationToken);
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
