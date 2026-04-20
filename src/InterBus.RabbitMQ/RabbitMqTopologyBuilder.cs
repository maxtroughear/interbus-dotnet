using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace InterBus.RabbitMQ;

internal sealed class RabbitMqTopologyBuilder(
    RabbitMqConnectionProvider connectionProvider,
    RabbitMqTransportOptions options,
    InterBusOptions busOptions,
    ILogger<RabbitMqTopologyBuilder> logger)
    : ITopologyBuilder
{
    public async Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        await using var connection = await ConnectWithRetryAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        foreach (var descriptor in busOptions.Consumers)
        {
            await channel.QueueDeclareAsync(
                queue: descriptor.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

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
            }
        }
    }

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
                    "InterBus RabbitMQ: topology connection failed; retrying in {Delay}",
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
}
