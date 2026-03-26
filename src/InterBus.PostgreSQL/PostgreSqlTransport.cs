using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

/// <summary>
/// PostgreSQL-backed <see cref="IInterBusTransport"/>.
/// <para>
/// Each <see cref="PublishToTopicAsync"/> call fans out the serialised payload to every queue
/// subscribed to the topic, inserting a row in the <c>messages</c> table and issuing a
/// <c>NOTIFY</c> on the queue's channel so that the <see cref="PostgreSqlMessagePump"/> wakes
/// up immediately. <see cref="SendToQueueAsync"/> inserts directly into the target queue.
/// </para>
/// <para>
/// Topic-to-queue routing is computed at construction time from <see cref="InterBusOptions"/>
/// (respecting any <see cref="InterBusOptions.TryGetTopicName"/> overrides) and held in memory
/// for the lifetime of the application — identical to the InMemory transport design.
/// </para>
/// </summary>
public sealed class PostgreSqlTransport : IInterBusTransport
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;
    private readonly Dictionary<string, List<string>> _topicQueues;

    internal IReadOnlyDictionary<string, List<string>> TopicQueues => _topicQueues;

    public PostgreSqlTransport(
        NpgsqlDataSource dataSource,
        PostgreSqlTransportOptions options,
        InterBusOptions busOptions)
    {
        _dataSource = dataSource;
        _schema = options.SchemaName;

        // Build topic → subscriber-queue mapping from consumer registrations.
        _topicQueues = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        foreach (var descriptor in busOptions.Consumers)
        {
            foreach (var type in descriptor.MessageTypes)
            {
                var topicName = busOptions.TryGetTopicName(type, out var custom)
                    ? custom
                    : RoutingConventions.TopicName(type);

                if (!_topicQueues.TryGetValue(topicName, out var queues))
                {
                    queues = [];
                    _topicQueues[topicName] = queues;
                }

                if (!queues.Contains(descriptor.QueueName))
                    queues.Add(descriptor.QueueName);
            }
        }
    }

    /// <inheritdoc/>
    public async Task PublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        if (!_topicQueues.TryGetValue(topicName, out var queues))
            return;

        foreach (var queueName in queues)
            await SendToQueueAsync(queueName, payload, headers, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        var headersJson = SerializeHeaders(headers);
        var channel = ChannelName(queueName);

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await using var insertCmd = conn.CreateCommand();
        insertCmd.Transaction = tx;
        insertCmd.CommandText =
            $"INSERT INTO \"{EscapeIdentifier(_schema)}\".messages (queue_name, payload, headers) " +
            "VALUES ($1, $2, $3)";
        insertCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        insertCmd.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = payload, NpgsqlDbType = NpgsqlDbType.Bytea });
        insertCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = headersJson });
        await insertCmd.ExecuteNonQueryAsync(cancellationToken);

        // NOTIFY is committed atomically with the INSERT: the notification is only sent to
        // listeners when this transaction commits, guaranteeing at-least-once delivery.
        await using var notifyCmd = conn.CreateCommand();
        notifyCmd.Transaction = tx;
        notifyCmd.CommandText = $"NOTIFY \"{channel}\"";
        await notifyCmd.ExecuteNonQueryAsync(cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// Derives a stable PostgreSQL NOTIFY/LISTEN channel identifier from a queue name.
    /// Channel names are limited to 63 bytes in PostgreSQL, so the queue name is SHA-256
    /// hashed to produce a short, collision-resistant, lowercase-alphanumeric identifier.
    /// </summary>
    internal static string ChannelName(string queueName)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(queueName));
        // "ib_" prefix (3) + 58 hex digits = 61 chars, safely under the 63-byte limit.
        return "ib_" + Convert.ToHexString(hash)[..58].ToLowerInvariant();
    }

    internal static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize<Dictionary<string, string>>(headers);

    internal static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(json)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }

    private static string EscapeIdentifier(string name) =>
        name.Replace("\"", "\"\"");
}
