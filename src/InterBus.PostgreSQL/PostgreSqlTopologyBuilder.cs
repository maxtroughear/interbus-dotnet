using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace InterBus.PostgreSQL;

internal sealed class PostgreSqlTopologyBuilder(
    NpgsqlDataSource dataSource,
    PostgreSqlTransportOptions options,
    InterBusOptions busOptions,
    ILogger<PostgreSqlTopologyBuilder> logger)
    : ITopologyBuilder
{
    private readonly ConcurrentDictionary<string, Task<long>> _queueIds = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Task<long>> _topicIds = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Task> _subscriptions = new(StringComparer.Ordinal);

    public async Task BuildTopologyAsync(CancellationToken cancellationToken)
    {
        foreach (var descriptor in busOptions.Consumers)
        {
            var queueId = await EnsureQueueIdAsync(descriptor.QueueName, cancellationToken);

            foreach (var messageType in descriptor.MessageTypes)
            {
                var topicName = busOptions.GetPublisherDescriptor(messageType).TopicName;

                await EnsureSubscriptionAsync(queueId, topicName, cancellationToken);
            }
        }
    }

    public async Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken)
    {
        await EnsureTopicIdAsync(rawTopicName, cancellationToken);
        return rawTopicName;
    }

    private Task<long> EnsureQueueIdAsync(string queueName, CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(_queueIds, queueName, ct =>
            PostgreSqlConnectionRetry.ExecuteAsync(
                retryCt => EnsureQueueIdCoreAsync(queueName, retryCt),
                options,
                logger,
                ct), cancellationToken);

    private Task<long> EnsureTopicIdAsync(string topicName, CancellationToken cancellationToken) =>
        EnsureWithCacheAsync(_topicIds, topicName, ct =>
            PostgreSqlConnectionRetry.ExecuteAsync(
                retryCt => EnsureTopicIdCoreAsync(topicName, retryCt),
                options,
                logger,
                ct), cancellationToken);

    private Task EnsureSubscriptionAsync(long queueId, string topicName, CancellationToken cancellationToken)
    {
        var key = queueId + "|" + topicName;
        return EnsureWithCacheAsync(_subscriptions, key, ct =>
            PostgreSqlConnectionRetry.ExecuteAsync(
                retryCt => EnsureSubscriptionCoreAsync(queueId, topicName, retryCt),
                options,
                logger,
                ct), cancellationToken);
    }

    private async Task<long> EnsureQueueIdCoreAsync(string queueName, CancellationToken cancellationToken)
    {
        await using var cmd = dataSource.CreateCommand();
        cmd.CommandText = $"SELECT \"{EscapeIdentifier(options.SchemaName)}\".ensure_queue($1)";
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        return (long)(await cmd.ExecuteScalarAsync(cancellationToken))!;
    }

    private async Task<long> EnsureTopicIdCoreAsync(string topicName, CancellationToken cancellationToken)
    {
        await using var cmd = dataSource.CreateCommand();
        cmd.CommandText = $"SELECT \"{EscapeIdentifier(options.SchemaName)}\".ensure_topic($1)";
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = topicName });
        return (long)(await cmd.ExecuteScalarAsync(cancellationToken))!;
    }

    private async Task EnsureSubscriptionCoreAsync(long queueId, string topicName, CancellationToken cancellationToken)
    {
        await using var cmd = dataSource.CreateCommand();
        cmd.CommandText = $"SELECT \"{EscapeIdentifier(options.SchemaName)}\".ensure_subscription($1, $2)";
        cmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queueId });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = topicName });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<TValue> EnsureWithCacheAsync<TValue>(
        ConcurrentDictionary<string, Task<TValue>> cache,
        string key,
        Func<CancellationToken, Task<TValue>> factory,
        CancellationToken cancellationToken)
    {
        var task = cache.GetOrAdd(key, _ => factory(CancellationToken.None));

        try
        {
            return await task.WaitAsync(cancellationToken);
        }
        catch
        {
            if (task.IsFaulted || task.IsCanceled)
                cache.TryRemove(key, out _);
            throw;
        }
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

    private static string EscapeIdentifier(string name) =>
        name.Replace("\"", "\"\"");
}
