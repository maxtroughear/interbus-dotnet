using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace InterBus.RabbitMQ;

internal sealed class RabbitMqTopologyBuilder(
    RabbitMqConnectionProvider connectionProvider,
    RabbitMqTransportOptions options,
    InterBusOptions busOptions,
    ILogger<RabbitMqTopologyBuilder> logger)
    : ITopologyBuilder, IDisposable
{
    private HashSet<string> DeclaredTopics { get; } = new(StringComparer.Ordinal);
    private HashSet<string> DeclaredQueues { get; } = new(StringComparer.Ordinal);
    private HashSet<string> DeclaredBindings { get; } = new(StringComparer.Ordinal);
    private SemaphoreSlim TopicLock { get; } = new(1, 1);
    private SemaphoreSlim QueueLock { get; } = new(1, 1);
    private SemaphoreSlim BindingLock { get; } = new(1, 1);

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
        lock (DeclaredTopics)
            DeclaredTopics.Remove(exchangeName);

        lock (DeclaredBindings)
            DeclaredBindings.RemoveWhere(k => k.StartsWith(exchangeName + "|", StringComparison.Ordinal));
    }

    internal void InvalidateQueue(string queueName)
    {
        lock (DeclaredQueues)
            DeclaredQueues.Remove(queueName);

        lock (DeclaredBindings)
            DeclaredBindings.RemoveWhere(k => k.EndsWith("|" + queueName, StringComparison.Ordinal));
    }

    private async Task<string> EnsureTopicDeclaredOnChannelAsync(
        IChannel channel,
        string rawTopicName,
        CancellationToken cancellationToken)
    {
        var exchangeName = options.ExchangePrefix + rawTopicName;
        await TopicLock.WaitAsync(cancellationToken);
        try
        {
            lock (DeclaredTopics)
            {
                if (DeclaredTopics.Contains(exchangeName))
                    return exchangeName;
            }

            await channel.ExchangeDeclareAsync(
                exchange: exchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            lock (DeclaredTopics)
                DeclaredTopics.Add(exchangeName);

            return exchangeName;
        }
        finally
        {
            TopicLock.Release();
        }
    }

    private async Task<string> EnsureQueueDeclaredOnChannelAsync(
        IChannel channel,
        string queueName,
        CancellationToken cancellationToken)
    {
        await QueueLock.WaitAsync(cancellationToken);
        try
        {
            lock (DeclaredQueues)
            {
                if (DeclaredQueues.Contains(queueName))
                    return queueName;
            }

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            lock (DeclaredQueues)
                DeclaredQueues.Add(queueName);

            return queueName;
        }
        finally
        {
            QueueLock.Release();
        }
    }

    private async Task EnsureQueueBindingAsync(
        IChannel channel,
        string queueName,
        string exchangeName,
        CancellationToken cancellationToken)
    {
        var bindingKey = exchangeName + "|" + queueName;
        await BindingLock.WaitAsync(cancellationToken);
        try
        {
            lock (DeclaredBindings)
            {
                if (DeclaredBindings.Contains(bindingKey))
                    return;
            }

            await channel.QueueBindAsync(
                queue: queueName,
                exchange: exchangeName,
                routingKey: string.Empty,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            lock (DeclaredBindings)
                DeclaredBindings.Add(bindingKey);
        }
        finally
        {
            BindingLock.Release();
        }
    }

    public void Dispose()
    {
        TopicLock.Dispose();
        QueueLock.Dispose();
        BindingLock.Dispose();
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
