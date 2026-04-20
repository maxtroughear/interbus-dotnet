using System.Collections.Concurrent;
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
    private ConcurrentDictionary<string, byte> DeclaredTopics { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, byte> DeclaredQueues { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, byte> DeclaredBindings { get; } = new(StringComparer.Ordinal);

    public async Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        await using var connection = await ConnectWithRetryAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = await EnsureQueueDeclaredOnChannelAsync(channel, descriptor.QueueName, cancellationToken);

            foreach (var messageType in descriptor.MessageTypes)
            {
                var topicName = busOptions.TryGetTopicName(messageType, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(messageType);

                var exchangeName = await EnsureTopicDeclaredOnChannelAsync(channel, topicName, cancellationToken);
                await EnsureQueueBindingAsync(channel, queueName, exchangeName, cancellationToken);
            }
        }
    }

    public async Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        await ExecuteWithRetryAsync(
            async ct =>
            {
                var connection = await connectionProvider.GetPublishConnectionAsync(ct);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: ct);
                await EnsureTopicDeclaredOnChannelAsync(channel, rawTopicName, ct);
            },
            cancellationToken);

        return options.ExchangePrefix + rawTopicName;
    }

    public async Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken)
    {
        await ExecuteWithRetryAsync(
            async ct =>
            {
                var connection = await connectionProvider.GetPublishConnectionAsync(ct);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: ct);
                await EnsureQueueDeclaredOnChannelAsync(channel, rawQueueName, ct);
            },
            cancellationToken);

        return rawQueueName;
    }

    internal void InvalidateTopic(string exchangeName)
    {
        DeclaredTopics.TryRemove(exchangeName, out _);

        foreach (var key in DeclaredBindings.Keys.Where(k => k.StartsWith(exchangeName + "|", StringComparison.Ordinal)))
            DeclaredBindings.TryRemove(key, out _);
    }

    internal void InvalidateQueue(string queueName)
    {
        DeclaredQueues.TryRemove(queueName, out _);

        foreach (var key in DeclaredBindings.Keys.Where(k => k.EndsWith("|" + queueName, StringComparison.Ordinal)))
            DeclaredBindings.TryRemove(key, out _);
    }

    private async Task<string> EnsureTopicDeclaredOnChannelAsync(
        IChannel channel,
        string rawTopicName,
        CancellationToken cancellationToken)
    {
        var exchangeName = options.ExchangePrefix + rawTopicName;
        if (DeclaredTopics.ContainsKey(exchangeName))
            return exchangeName;

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: cancellationToken);

        DeclaredTopics.TryAdd(exchangeName, 0);
        return exchangeName;
    }

    private async Task<string> EnsureQueueDeclaredOnChannelAsync(
        IChannel channel,
        string queueName,
        CancellationToken cancellationToken)
    {
        if (DeclaredQueues.ContainsKey(queueName))
            return queueName;

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: cancellationToken);

        DeclaredQueues.TryAdd(queueName, 0);
        return queueName;
    }

    private async Task EnsureQueueBindingAsync(
        IChannel channel,
        string queueName,
        string exchangeName,
        CancellationToken cancellationToken)
    {
        var bindingKey = exchangeName + "|" + queueName;
        if (DeclaredBindings.ContainsKey(bindingKey))
            return;

        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: string.Empty,
            arguments: null,
            noWait: false,
            cancellationToken: cancellationToken);

        DeclaredBindings.TryAdd(bindingKey, 0);
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

    private async Task ExecuteWithRetryAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await operation(cancellationToken);
                return;
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
                    "InterBus RabbitMQ: topology declaration failed; retrying in {Delay}",
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
