using InterBus.Consumer;
using InterBus.DeadLettering;
using InterBus.Dispatch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace InterBus.RabbitMQ;

/// <summary>
/// Service Activator (Message Pump). A <see cref="BackgroundService"/> that starts one consumer
/// per registered InterBus consumer queue.
/// <para>Topology is created or verified during bus startup before this pump starts.</para>
/// <para>
/// Successful dispatch acknowledges the RabbitMQ message. Unhandled consumer failures are
/// republished to a dead-letter exchange with diagnostic headers; other failures are negatively
/// acknowledged with <c>requeue=true</c> for redelivery.
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
    /// Opens the consume connection before the background loop starts.
    /// </summary>
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (busOptions.Consumers.Count > 0)
        {
            _connection = await ConnectWithRetryAsync(cancellationToken);
            logger.LogDebug("InterBus RabbitMQ: consume connection established");
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
            catch (DeadLetterException deadLetterException)
            {
                await DeadLetterAsync(channel, descriptor, ea, deadLetterException);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "InterBus RabbitMQ: consumer {ConsumerType} failed for queue {QueueName}; requeueing message for redelivery",
                    descriptor.ConsumerType.Name,
                    descriptor.QueueName);

                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true);
            }

            // ReSharper restore MethodSupportsCancellation
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
        var headers = RabbitMqHeaders.ParseAmqpHeaders(ea.BasicProperties.Headers);
        var context = new DispatchContext(descriptor, ea.Body.ToArray(), headers);
        await executor.ExecuteAsync(context, ct);
    }

    private async Task DeadLetterAsync(
        IChannel consumeChannel,
        ConsumerDescriptor descriptor,
        BasicDeliverEventArgs ea,
        DeadLetterException ex)
    {
        var deadLetterExchangeName = descriptor.QueueName + options.DeadLetterExchangeSuffix;
        var publishConnection = await connectionProvider.GetPublishConnectionAsync(CancellationToken.None);
        await using var publishChannel = await publishConnection.CreateChannelAsync(cancellationToken: CancellationToken.None);
        var properties = new BasicProperties
        {
            ContentType = ea.BasicProperties.ContentType,
            ContentEncoding = ea.BasicProperties.ContentEncoding,
            DeliveryMode = ea.BasicProperties.DeliveryMode,
            Priority = ea.BasicProperties.Priority,
            CorrelationId = ea.BasicProperties.CorrelationId,
            MessageId = ea.BasicProperties.MessageId,
            Timestamp = ea.BasicProperties.Timestamp,
            Type = ea.BasicProperties.Type,
            AppId = ea.BasicProperties.AppId,
            Headers = ea.BasicProperties.Headers is null
                ? new Dictionary<string, object?>()
                : new Dictionary<string, object?>(ea.BasicProperties.Headers, StringComparer.Ordinal),
        };

        properties.Headers[InterBusHeaders.DeadLetterReason] = ex.Reason;
        properties.Headers[InterBusHeaders.DeadLetterError] = ex.Error;
        properties.Headers[InterBusHeaders.DeadLetteredAt] = ex.DeadLetteredAtUtc.ToString("O");

        await publishChannel.BasicPublishAsync(
            exchange: deadLetterExchangeName,
            routingKey: descriptor.QueueName,
            mandatory: false,
            basicProperties: properties,
            body: ea.Body,
            cancellationToken: CancellationToken.None);

        await consumeChannel.BasicAckAsync(ea.DeliveryTag, multiple: false);

        logger.LogWarning(
            ex,
            "InterBus RabbitMQ: consumer {ConsumerType} failed for queue {QueueName}; message moved to dead-letter exchange {DeadLetterExchange}",
            descriptor.ConsumerType.Name,
            descriptor.QueueName,
            deadLetterExchangeName);
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
