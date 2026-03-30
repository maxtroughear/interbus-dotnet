using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

/// <summary>
/// PostgreSQL-backed <see cref="IInterBusTransport"/>.
/// <para>
/// <see cref="PublishToTopicAsync"/> delegates entirely to the <c>publish_to_topic</c> stored
/// function, which looks up subscribers, inserts the payload once, fans-out routing rows, and
/// issues a <c>NOTIFY</c> per queue — all in a single round trip.
/// </para>
/// <para>
/// <see cref="SendToQueueAsync"/> delegates to the <c>send_to_queue</c> stored function, which
/// inserts the payload and routing row and issues a <c>NOTIFY</c> in a single round trip. The
/// function assumes the queue already exists (registered by the consumer pump at startup) and
/// only falls back to an insert when it does not.
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
        var headersJson = SerializeHeaders(headers);
        await using var cmd = _dataSource.CreateCommand();
        cmd.CommandText = $"SELECT \"{EscapeIdentifier(_schema)}\".publish_to_topic($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = topicName });
        cmd.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = payload, NpgsqlDbType = NpgsqlDbType.Bytea });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = headersJson });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public async Task SendToQueueAsync(string queueName, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        var headersJson = SerializeHeaders(headers);
        await using var cmd = _dataSource.CreateCommand();
        cmd.CommandText = $"SELECT \"{EscapeIdentifier(_schema)}\".send_to_queue($1, $2, $3)";
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = queueName });
        cmd.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = payload, NpgsqlDbType = NpgsqlDbType.Bytea });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = headersJson });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
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
