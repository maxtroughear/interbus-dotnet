using InterBus.Middleware;
using InterBus.Retry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

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
/// <para>
/// Initial connection failures are retried using the exponential back-off configured in
/// <see cref="RabbitMqTransportOptions.ConnectionRetry"/>.
/// </para>
/// </summary>
internal sealed class RabbitMqMessagePump(
    RabbitMqConnectionProvider connectionProvider,
    RabbitMqTransportOptions options,
    InterBusOptions busOptions,
    IServiceScopeFactory scopeFactory,
    ILogger<RabbitMqMessagePump> logger)
    : BackgroundService
{
    private IConnection? _connection;

    /// <summary>
    /// Opens the consume connection and declares the full RabbitMQ topology before the
    /// background loop starts. By completing topology setup here, publishers can route
    /// messages as soon as <see cref="StartAsync"/> returns without risking a race where
    /// a publish happens before the exchange bindings exist (which would silently drop the message
    /// on a fanout exchange with no bound queues).
    /// <para>
    /// Initial connection attempts are retried using the exponential back-off policy defined
    /// in <see cref="RabbitMqTransportOptions.ConnectionRetry"/>.
    /// </para>
    /// </summary>
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (busOptions.Consumers.Count > 0)
        {
            _connection = await ConnectWithRetryAsync(cancellationToken);
            logger.LogDebug("InterBus RabbitMQ: consume connection established");
            await DeclareTopologyAsync(cancellationToken);
        }

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = busOptions.Consumers;
        if (consumers.Count == 0)
        {
            logger.LogInformation("InterBus RabbitMQ: no consumers registered; message pump idle");
            return;
        }

        // Start one consumer per queue; all run concurrently.
        var tasks = consumers.Select(d => ConsumeQueueAsync(d, stoppingToken));
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Establishes the consume connection, retrying on transient connection failures using
    /// the policy in <see cref="RabbitMqTransportOptions.ConnectionRetry"/>.
    /// </summary>
    private async Task<IConnection> ConnectWithRetryAsync(CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await connectionProvider.CreateConsumeConnectionAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsConnectionRelated(ex) && ShouldRetry(ex, attempt + 1))
            {
                var delay = options.ConnectionRetry.DelayProvider(++attempt);
                logger.LogWarning(
                    ex,
                    "InterBus RabbitMQ: consume connection failed; retrying in {Delay}",
                    delay);
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private bool ShouldRetry(Exception ex, int attempt)
    {
        var retry = options.ConnectionRetry;
        if (attempt > retry.RetryCount)
            return false;
        return retry.ShouldRetry?.Invoke(ex) ?? true;
    }

    private static bool IsConnectionRelated(Exception ex) =>
        ex is BrokerUnreachableException
            or ConnectFailureException
            or System.Net.Sockets.SocketException
            or TimeoutException
        || (ex.InnerException is not null && IsConnectionRelated(ex.InnerException));

    /// <summary>
    /// Declares all queues, exchanges, and bindings. Idempotent — safe to call even if the
    /// topology already exists from a previous run. Uses a short-lived channel that is closed
    /// after setup; the per-consumer channels are created later in
    /// <see cref="ConsumeQueueAsync"/>.
    /// </summary>
    private async Task DeclareTopologyAsync(CancellationToken cancellationToken)
    {
        await using var channel = await _connection!.CreateChannelAsync(cancellationToken: cancellationToken);

        foreach (var descriptor in busOptions.Consumers)
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
                var topicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var exchangeName = options.ExchangePrefix + topicName;

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

                logger.LogDebug(
                    "InterBus RabbitMQ: bound queue {QueueName} to exchange {ExchangeName}",
                    descriptor.QueueName,
                    exchangeName);
            }
        }
    }

    private async Task ConsumeQueueAsync(ConsumerDescriptor descriptor, CancellationToken stoppingToken)
    {
        logger.LogDebug(
            "InterBus RabbitMQ: starting consumer for queue {QueueName}",
            descriptor.QueueName);

        var concurrency = (ushort)Math.Max(1, descriptor.ConcurrencyCount);
        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            consumerDispatchConcurrency: concurrency);

        var channel = await _connection!.CreateChannelAsync(channelOptions, stoppingToken);

        // Prefetch one message at a time per concurrent handler for fair dispatch.
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: concurrency,
            global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            // ReSharper disable MethodSupportsCancellation
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
                logger.LogWarning(
                    ex,
                    "InterBus RabbitMQ: consumer {ConsumerType} failed for queue {QueueName}; requeueing message for redelivery",
                    descriptor.ConsumerType.Name,
                    descriptor.QueueName);

                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
                // ReSharper restore MethodSupportsCancellation
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

        logger.LogDebug(
            "InterBus RabbitMQ: consumer for queue {QueueName} stopped",
            descriptor.QueueName);

        await channel.CloseAsync(CancellationToken.None);
        await channel.DisposeAsync();
    }

    private async Task DispatchAsync(ConsumerDescriptor descriptor, BasicDeliverEventArgs ea, CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
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
