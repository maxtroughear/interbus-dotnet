using System.Text.Json;
using InterBus.Transport;
using Npgsql;
using NpgsqlTypes;

namespace InterBus.PostgreSQL;

/// <summary>
/// PostgreSQL-backed <see cref="ITransport"/>.
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
public sealed class PostgreSqlTransport(
    NpgsqlDataSource dataSource,
    PostgreSqlTransportOptions options,
    ITopologyBuilder topologyBuilder)
    : ITransport
{
    /// <summary>URI scheme for PostgreSQL topic (fan-out) destinations.</summary>
    public const string TopicScheme = "postgresqltopic";

    /// <summary>URI scheme for PostgreSQL queue (point-to-point) destinations.</summary>
    public const string QueueScheme = "postgresqlqueue";

    private readonly string _schema = options.SchemaName;

    /// <inheritdoc/>
    public async Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
    {
        var name = RoutingConventions.GetDestinationName(context.Destination);
        var headersJson = SerializeHeaders(context.Headers);
        var visibleAt = context.Delay.HasValue
            ? (DateTimeOffset?)DateTimeOffset.UtcNow.Add(context.Delay.Value)
            : null;

        if (context.Destination.Scheme == TopicScheme)
            await topologyBuilder.EnsureTopicAsync(name, cancellationToken);
        else if (context.Destination.Scheme == QueueScheme)
            await topologyBuilder.EnsureQueueAsync(name, cancellationToken);
        else
            throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{TopicScheme}' or '{QueueScheme}'.", nameof(context));

        await using var cmd = dataSource.CreateCommand();

        cmd.CommandText = context.Destination.Scheme switch
        {
            TopicScheme => $"SELECT \"{EscapeIdentifier(_schema)}\".publish_to_topic($1, $2, $3, $4)",
            QueueScheme => $"SELECT \"{EscapeIdentifier(_schema)}\".send_to_queue($1, $2, $3, $4)",
            _ => throw new ArgumentException(
                $"Unsupported destination URI scheme '{context.Destination.Scheme}'. " +
                $"Expected '{TopicScheme}' or '{QueueScheme}'.", nameof(context))
        };

        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });
        cmd.Parameters.Add(new NpgsqlParameter<byte[]>
            { TypedValue = context.Payload, NpgsqlDbType = NpgsqlDbType.Bytea });
        cmd.Parameters.Add(new NpgsqlParameter<string> { TypedValue = headersJson });
        cmd.Parameters.Add(visibleAt.HasValue
            ? new NpgsqlParameter<DateTimeOffset>
                { TypedValue = visibleAt.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz }
            : new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string SerializeHeaders(MessageHeaders headers) =>
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
