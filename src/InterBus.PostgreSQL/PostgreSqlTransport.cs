using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

/// <summary>
/// PostgreSQL-backed <see cref="IInterBusTransport"/>.
/// <para>
/// <see cref="PublishToTopicAsync"/> looks up subscriber queues from the
/// <c>subscriptions</c> table (populated by each consumer service at startup),
/// then inserts one row per subscriber queue in the <c>messages</c> table and issues a
/// <c>NOTIFY</c> on each queue's channel. This decouples the publisher from consumer
/// registrations: publishers do not need to know which consumers exist.
/// </para>
/// <para>
/// <see cref="SendToQueueAsync"/> inserts directly into the named queue, bypassing
/// topic routing entirely.
/// </para>
/// </summary>
public sealed class PostgreSqlTransport : IInterBusTransport
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;

    public PostgreSqlTransport(NpgsqlDataSource dataSource, PostgreSqlTransportOptions options)
    {
        _dataSource = dataSource;
        _schema = options.SchemaName;
    }

    /// <inheritdoc/>
    public async Task PublishToTopicAsync(string topicName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        // Read subscriber queues and their numeric IDs from the DB — this decouples publishers
        // from consumers and supports consumers deployed in separate services/processes.
        var queues = await GetSubscriberQueuesAsync(topicName, cancellationToken);
        if (queues.Count == 0) return;

        var headersJson = SerializeHeaders(headers);

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Store the payload exactly once, regardless of how many subscriber queues there are.
        await using var payloadCmd = conn.CreateCommand();
        payloadCmd.Transaction = tx;
        payloadCmd.CommandText =
            $"INSERT INTO \"{EscapeIdentifier(_schema)}\".message_payloads (payload, headers) " +
            "VALUES ($1, $2) RETURNING id";
        payloadCmd.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = payload, NpgsqlDbType = NpgsqlDbType.Bytea });
        payloadCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = headersJson });
        var payloadId = (long)(await payloadCmd.ExecuteScalarAsync(cancellationToken))!;

        // Insert one routing row per subscriber queue and NOTIFY each queue's channel.
        // The payload bytes are not duplicated — each row carries only a FK reference.
        await using var insertCmd = conn.CreateCommand();
        insertCmd.Transaction = tx;
        insertCmd.CommandText =
            $"INSERT INTO \"{EscapeIdentifier(_schema)}\".messages (queue_name, payload_id) " +
            "VALUES ($1, $2)";
        var pQueue = new NpgsqlParameter<string>();
        var pPayloadId = new NpgsqlParameter<long>();
        insertCmd.Parameters.Add(pQueue);
        insertCmd.Parameters.Add(pPayloadId);
        await insertCmd.PrepareAsync(cancellationToken);

        await using var notifyCmd = conn.CreateCommand();
        notifyCmd.Transaction = tx;

        foreach (var (queueName, queueId) in queues)
        {
            pQueue.TypedValue = queueName;
            pPayloadId.TypedValue = payloadId;
            await insertCmd.ExecuteNonQueryAsync(cancellationToken);

            // Channel names are "ib_<queue_id>" — stable, DB-assigned identifiers that are
            // always valid PostgreSQL identifiers and safely under the 63-byte limit.
            notifyCmd.CommandText = $"NOTIFY \"ib_{queueId}\"";
            await notifyCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        var headersJson = SerializeHeaders(headers);

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Ensure the queue row exists and obtain its stable numeric channel ID.
        await using var queueCmd = conn.CreateCommand();
        queueCmd.Transaction = tx;
        queueCmd.CommandText =
            $"INSERT INTO \"{EscapeIdentifier(_schema)}\".queues (queue_name) VALUES ($1) " +
            "ON CONFLICT (queue_name) DO UPDATE SET queue_name = EXCLUDED.queue_name RETURNING id";
        queueCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        var queueId = (long)(await queueCmd.ExecuteScalarAsync(cancellationToken))!;

        await using var payloadCmd = conn.CreateCommand();
        payloadCmd.Transaction = tx;
        payloadCmd.CommandText =
            $"INSERT INTO \"{EscapeIdentifier(_schema)}\".message_payloads (payload, headers) " +
            "VALUES ($1, $2) RETURNING id";
        payloadCmd.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = payload, NpgsqlDbType = NpgsqlDbType.Bytea });
        payloadCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = headersJson });
        var payloadId = (long)(await payloadCmd.ExecuteScalarAsync(cancellationToken))!;

        await using var insertCmd = conn.CreateCommand();
        insertCmd.Transaction = tx;
        insertCmd.CommandText =
            $"INSERT INTO \"{EscapeIdentifier(_schema)}\".messages (queue_name, payload_id) VALUES ($1, $2)";
        insertCmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        insertCmd.Parameters.Add(new NpgsqlParameter<long> { TypedValue = payloadId });
        await insertCmd.ExecuteNonQueryAsync(cancellationToken);

        // NOTIFY is committed atomically with the INSERT: the notification is only sent to
        // listeners when this transaction commits, guaranteeing at-least-once delivery.
        await using var notifyCmd = conn.CreateCommand();
        notifyCmd.Transaction = tx;
        notifyCmd.CommandText = $"NOTIFY \"ib_{queueId}\"";
        await notifyCmd.ExecuteNonQueryAsync(cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// Reads the list of subscriber queue names and their numeric channel IDs for the given
    /// topic from the <c>subscriptions</c> and <c>queues</c> tables.
    /// </summary>
    private async Task<List<(string QueueName, long QueueId)>> GetSubscriberQueuesAsync(
        string topicName, CancellationToken cancellationToken)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT s.queue_name, q.id " +
            $"FROM \"{EscapeIdentifier(_schema)}\".subscriptions s " +
            $"JOIN \"{EscapeIdentifier(_schema)}\".queues q ON q.queue_name = s.queue_name " +
            "WHERE s.topic_name = $1";
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = topicName });

        var queues = new List<(string, long)>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            queues.Add((reader.GetString(0), reader.GetInt64(1)));
        return queues;
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
