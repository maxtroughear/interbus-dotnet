using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Applies the InterBus PostgreSQL schema migration at application startup.
/// Creates the messages table and index inside the configured schema if they do not already exist.
/// Idempotent: safe to run on every start.
/// </summary>
internal sealed class PostgreSqlSchemaManager
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _schema;

    public PostgreSqlSchemaManager(NpgsqlDataSource dataSource, PostgreSqlTransportOptions options)
    {
        _dataSource = dataSource;
        _schema = options.SchemaName;
    }

    /// <summary>Ensures the schema, tables and indexes exist. Idempotent.</summary>
    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        var schema = QuoteIdentifier(_schema);

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"CREATE SCHEMA IF NOT EXISTS {schema};\n" +
            // Each distinct queue is assigned a stable numeric ID used as the NOTIFY/LISTEN channel.
            $"CREATE TABLE IF NOT EXISTS {schema}.queues (\n" +
            "    id         BIGSERIAL PRIMARY KEY,\n" +
            "    queue_name TEXT      NOT NULL UNIQUE\n" +
            ");\n" +
            // Payload stored once; queue routing rows reference it via FK.
            $"CREATE TABLE IF NOT EXISTS {schema}.message_payloads (\n" +
            "    id         BIGSERIAL PRIMARY KEY,\n" +
            "    payload    BYTEA       NOT NULL,\n" +
            "    headers    TEXT        NOT NULL,\n" +
            "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
            ");\n" +
            $"CREATE TABLE IF NOT EXISTS {schema}.messages (\n" +
            "    id         BIGSERIAL PRIMARY KEY,\n" +
            "    queue_name TEXT        NOT NULL,\n" +
            $"    payload_id BIGINT      NOT NULL REFERENCES {schema}.message_payloads(id),\n" +
            "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
            ");\n" +
            $"CREATE INDEX IF NOT EXISTS ix_messages_queue_pending\n" +
            $"    ON {schema}.messages (queue_name, id);\n" +
            $"CREATE TABLE IF NOT EXISTS {schema}.subscriptions (\n" +
            "    queue_name TEXT NOT NULL,\n" +
            "    topic_name TEXT NOT NULL,\n" +
            "    PRIMARY KEY (queue_name, topic_name)\n" +
            ");";
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string QuoteIdentifier(string name) =>
        $"\"{name.Replace("\"", "\"\"")}\"";
}
