using System.Collections.Generic;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

/// <summary>
/// PostgreSQL-backed <see cref="IInterBusTransport"/>.
/// <para>
/// When <see cref="SendAsync"/> is called with a <c>postgresqltopic://</c> destination URI,
/// it delegates to the <c>publish_to_topic</c> stored function, which looks up subscribers,
/// inserts the payload once, fans-out routing rows, and issues a <c>NOTIFY</c> per queue —
/// all in a single round trip.
/// </para>
/// <para>
/// When called with a <c>postgresqlqueue://</c> destination URI, it delegates to the
/// <c>send_to_queue</c> stored function, which inserts the payload and routing row and
/// issues a <c>NOTIFY</c> in a single round trip.
/// </para>
/// </summary>
public sealed class PostgreSqlTransport : IInterBusTransport
{
    /// <summary>URI scheme for PostgreSQL topic (fan-out) destinations.</summary>
    public const string TopicScheme = "postgresqltopic";

    /// <summary>URI scheme for PostgreSQL queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "postgresqlqueue";

    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;

    public PostgreSqlTransport(NpgsqlDataSource dataSource, PostgreSqlTransportOptions options)
    {
        _dataSource = dataSource;
        _schema = options.SchemaName;
    }

    /// <inheritdoc/>
    public async Task SendAsync(Uri destination, byte[] payload, MessageHeaders headers,
        CancellationToken cancellationToken)
    {
        var name = RoutingConventions.GetDestinationName(destination);
        var headersJson = SerializeHeaders(headers);

        await using var cmd = _dataSource.CreateCommand();

        if (destination.Scheme == TopicScheme)
        {
            cmd.CommandText = $"SELECT \"{EscapeIdentifier(_schema)}\".publish_to_topic($1, $2, $3)";
        }
        else if (destination.Scheme == QueueScheme)
        {
            cmd.CommandText = $"SELECT \"{EscapeIdentifier(_schema)}\".send_to_queue($1, $2, $3)";
        }
        else
        {
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{destination.Scheme}'. " +
                $"Expected '{TopicScheme}' or '{QueueScheme}'.",
                nameof(destination));
        }

        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });
        cmd.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = payload, NpgsqlDbType = NpgsqlDbType.Bytea });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = headersJson });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    internal static string SerializeHeaders(MessageHeaders headers) =>
        JsonSerializer.Serialize(headers, PostgreSqlJsonContext.Default.DictionaryStringString);

    internal static MessageHeaders DeserializeHeaders(string json)
    {
        var dict = JsonSerializer.Deserialize(json, PostgreSqlJsonContext.Default.DictionaryStringString)
                   ?? new Dictionary<string, string>();
        return new MessageHeaders(dict);
    }

    private static string EscapeIdentifier(string name) =>
        name.Replace("\"", "\"\"");
}
