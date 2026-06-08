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
    private ConcurrentDictionary<string, Task> DeclaredTopics { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, Task> DeclaredQueues { get; } = new(StringComparer.Ordinal);
    private ConcurrentDictionary<string, Task> DeclaredBindings { get; } = new(StringComparer.Ordinal);

    public async Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        await using var connection = await ConnectWithRetryAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        foreach (var descriptor in busOptions.Consumers)
        {
            var queueName = descriptor.QueueName;
            var deadLetterExchangeName = BuildDeadLetterExchangeName(queueName);
            var deadLetterQueueName = BuildDeadLetterQueueName(queueName);

            await EnsureDeadLetterExchangeDeclaredOnChannelAsync(channel, deadLetterExchangeName, cancellationToken);
            await EnsureDeadLetterQueueDeclaredOnChannelAsync(channel, deadLetterQueueName, cancellationToken);
            await EnsureDeadLetterBindingAsync(
                channel,
                deadLetterQueueName,
                deadLetterExchangeName,
                queueName,
                cancellationToken);

            await EnsureQueueDeclaredOnChannelAsync(channel, queueName, deadLetterExchangeName, cancellationToken);

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
        var exchangeName = options.ExchangePrefix + rawTopicName;
        await EnsureTopicDeclaredAsync(exchangeName, cancellationToken);
        return exchangeName;
    }

    internal void InvalidateTopic(string exchangeName)
    {
        DeclaredTopics.TryRemove(exchangeName, out _);

        foreach (var bindingKey in DeclaredBindings.Keys)
        {
            if (bindingKey.StartsWith(exchangeName + "|", StringComparison.Ordinal))
                DeclaredBindings.TryRemove(bindingKey, out _);
        }
    }

    private string BuildDeadLetterQueueName(string queueName) => queueName + options.DeadLetterQueueSuffix;

    private string BuildDeadLetterExchangeName(string queueName) => queueName + options.DeadLetterExchangeSuffix;

    private Task EnsureTopicDeclaredAsync(string exchangeName, CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(
            DeclaredTopics,
            exchangeName,
            async ct =>
            {
                await ExecuteWithRetryAsync(
                    async retryCt =>
                    {
                        var connection = await connectionProvider.GetPublishConnectionAsync(retryCt);
                        await using var channel = await connection.CreateChannelAsync(cancellationToken: retryCt);
                        await DeclareTopicOnChannelAsync(channel, exchangeName, retryCt);
                    },
                    ct);
            },
            cancellationToken);

    private async Task<string> EnsureTopicDeclaredOnChannelAsync(
        IChannel channel,
        string rawTopicName,
        CancellationToken cancellationToken)
    {
        var exchangeName = options.ExchangePrefix + rawTopicName;
        await EnsureWithCacheAsync(
            DeclaredTopics,
            exchangeName,
            ct => DeclareTopicOnChannelAsync(channel, exchangeName, ct),
            cancellationToken);
        return exchangeName;
    }

    private async Task EnsureQueueDeclaredOnChannelAsync(
        IChannel channel,
        string queueName,
        string deadLetterExchangeName,
        CancellationToken cancellationToken)
    {
        await EnsureWithCacheAsync(
            DeclaredQueues,
            queueName,
            ct => DeclareQueueOnChannelAsync(channel, queueName, deadLetterExchangeName, ct),
            cancellationToken);
    }

    private Task EnsureDeadLetterExchangeDeclaredOnChannelAsync(
        IChannel channel,
        string deadLetterExchangeName,
        CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(
            DeclaredTopics,
            "dlx|" + deadLetterExchangeName,
            ct => DeclareDeadLetterExchangeOnChannelAsync(channel, deadLetterExchangeName, ct),
            cancellationToken);

    private Task EnsureDeadLetterQueueDeclaredOnChannelAsync(
        IChannel channel,
        string deadLetterQueueName,
        CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(
            DeclaredQueues,
            "dlq|" + deadLetterQueueName,
            ct => DeclareDeadLetterQueueOnChannelAsync(channel, deadLetterQueueName, ct),
            cancellationToken);

    private Task EnsureDeadLetterBindingAsync(
        IChannel channel,
        string deadLetterQueueName,
        string deadLetterExchangeName,
        string sourceQueueName,
        CancellationToken cancellationToken)
    {
        var bindingKey = deadLetterExchangeName + "|" + deadLetterQueueName + "|" + sourceQueueName;
        return EnsureWithCacheAsync(
            DeclaredBindings,
            "dlb|" + bindingKey,
            ct => DeclareDeadLetterBindingOnChannelAsync(
                channel,
                deadLetterQueueName,
                deadLetterExchangeName,
                sourceQueueName,
                ct),
            cancellationToken);
    }

    private async Task EnsureQueueBindingAsync(
        IChannel channel,
        string queueName,
        string exchangeName,
        CancellationToken cancellationToken)
    {
        var bindingKey = exchangeName + "|" + queueName;
        await EnsureWithCacheAsync(
            DeclaredBindings,
            bindingKey,
            ct => DeclareBindingOnChannelAsync(channel, queueName, exchangeName, ct),
            cancellationToken);
    }

    private static async Task DeclareTopicOnChannelAsync(
        IChannel channel,
        string exchangeName,
        CancellationToken cancellationToken)
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

    private static async Task DeclareQueueOnChannelAsync(
        IChannel channel,
        string queueName,
        string deadLetterExchangeName,
        CancellationToken cancellationToken)
    {
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = deadLetterExchangeName,
            },
            noWait: false,
            cancellationToken: cancellationToken);
    }

    private static Task DeclareDeadLetterExchangeOnChannelAsync(
        IChannel channel,
        string deadLetterExchangeName,
        CancellationToken cancellationToken) =>
        channel.ExchangeDeclareAsync(
            exchange: deadLetterExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: cancellationToken);

    private static Task DeclareDeadLetterQueueOnChannelAsync(
        IChannel channel,
        string deadLetterQueueName,
        CancellationToken cancellationToken) =>
        channel.QueueDeclareAsync(
            queue: deadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: cancellationToken);

    private static Task DeclareBindingOnChannelAsync(
        IChannel channel,
        string queueName,
        string exchangeName,
        CancellationToken cancellationToken) =>
        channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: string.Empty,
            arguments: null,
            noWait: false,
            cancellationToken: cancellationToken);

    private static Task DeclareDeadLetterBindingOnChannelAsync(
        IChannel channel,
        string deadLetterQueueName,
        string deadLetterExchangeName,
        string sourceQueueName,
        CancellationToken cancellationToken) =>
        channel.QueueBindAsync(
            queue: deadLetterQueueName,
            exchange: deadLetterExchangeName,
            routingKey: sourceQueueName,
            arguments: null,
            noWait: false,
            cancellationToken: cancellationToken);

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


    private static async Task EnsureWithCacheAsync(
        ConcurrentDictionary<string, Task> cache,
        string key,
        Func<CancellationToken, Task> factory,
        CancellationToken cancellationToken)
    {
        var task = cache.GetOrAdd(key, _ => factory(CancellationToken.None));

        try
        {
            await task.WaitAsync(cancellationToken);
        }
        catch
        {
            if (task.IsFaulted || task.IsCanceled)
                cache.TryRemove(key, out _);

            throw;
        }
    }

    private static bool IsConnectionRelated(Exception ex) =>
        ex is BrokerUnreachableException
            or ConnectFailureException
            or System.Net.Sockets.SocketException
            or TimeoutException
        || (ex.InnerException is not null && IsConnectionRelated(ex.InnerException));
}
