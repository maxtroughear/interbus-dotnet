using System.Security.Cryptography;
using System.Text;
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
/// <para>
/// <see cref="EnqueueInTransactionAsync"/> and <see cref="PublishInTransactionAsync"/>
/// are non-interface overloads that accept a caller-owned <see cref="NpgsqlTransaction"/>,
/// enabling atomic publish-with-business-logic without an outbox pattern.
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
        // Read subscriber queues from the DB — this decouples publishers from consumers and
        // supports consumers deployed in separate services/processes.
        var queues = await GetSubscriberQueuesAsync(topicName, cancellationToken);
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

        await InsertMessageAsync(conn, tx, queueName, payload, headersJson, channel, cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// Publishes a message to all queues subscribed to <paramref name="topicName"/>, enlisting
    /// the INSERT and NOTIFY operations in the caller-supplied <paramref name="transaction"/>.
    /// The caller is responsible for committing or rolling back the transaction.
    /// </summary>
    /// <remarks>
    /// Use this overload to publish atomically alongside other database operations in the same
    /// transaction, avoiding the need for an outbox pattern. Subscriber queue lookup uses a
    /// separate connection so that it is not affected by the transaction isolation level of the
    /// caller's connection.
    /// </remarks>
    public async Task PublishInTransactionAsync(string topicName, byte[] payload, MessageHeaders headers,
        NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var queues = await GetSubscriberQueuesAsync(topicName, cancellationToken);
        foreach (var queueName in queues)
            await EnqueueInTransactionAsync(queueName, payload, headers, transaction, cancellationToken);
    }

    /// <summary>
    /// Inserts a message row and issues a NOTIFY for <paramref name="queueName"/>, enlisting
    /// both operations in the caller-supplied <paramref name="transaction"/>.
    /// The caller is responsible for committing or rolling back the transaction.
    /// </summary>
    /// <remarks>
    /// Use this overload to enqueue atomically alongside other database operations in the same
    /// transaction, avoiding the need for an outbox pattern.
    /// </remarks>
    public async Task EnqueueInTransactionAsync(string queueName, byte[] payload, MessageHeaders headers,
        NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var headersJson = SerializeHeaders(headers);
        var channel = ChannelName(queueName);
        var conn = transaction.Connection
                   ?? throw new InvalidOperationException(
                       "The supplied NpgsqlTransaction has no associated connection. " +
                       "Ensure the transaction is open and not disposed before calling this method.");

        await InsertMessageAsync(conn, transaction, queueName, payload, headersJson, channel, cancellationToken);
    }

    /// <summary>
    /// Reads the list of queue names subscribed to <paramref name="topicName"/> from the
    /// <c>subscriptions</c> table.
    /// </summary>
    private async Task<List<string>> GetSubscriberQueuesAsync(string topicName,
        CancellationToken cancellationToken)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT queue_name FROM \"{EscapeIdentifier(_schema)}\".subscriptions " +
            "WHERE topic_name = $1";
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = topicName });

        var queues = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            queues.Add(reader.GetString(0));
        return queues;
    }

    /// <summary>
    /// Executes the INSERT into <c>messages</c> and the NOTIFY using the supplied connection
    /// and transaction. Does not commit.
    /// </summary>
    private async Task InsertMessageAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        string queueName,
        byte[] payload,
        string headersJson,
        string channel,
        CancellationToken cancellationToken)
    {
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
