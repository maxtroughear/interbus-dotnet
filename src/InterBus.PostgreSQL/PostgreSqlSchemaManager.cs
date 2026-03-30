using Npgsql;

namespace InterBus.PostgreSQL;

/// <summary>
/// Applies the InterBus PostgreSQL schema migration at application startup.
/// Creates all tables, indexes, and stored functions inside the configured schema.
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

    /// <summary>Ensures the schema, tables, indexes and stored functions exist. Idempotent.</summary>
    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        var s = QuoteIdentifier(_schema);

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);

        // Step 1: schema, tables and indexes.
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText =
                $"CREATE SCHEMA IF NOT EXISTS {s};\n" +
                // Each distinct queue is assigned a stable numeric ID used as the NOTIFY/LISTEN channel.
                $"CREATE TABLE IF NOT EXISTS {s}.queues (\n" +
                "    id         BIGSERIAL PRIMARY KEY,\n" +
                "    queue_name TEXT      NOT NULL UNIQUE\n" +
                ");\n" +
                // Topics are normalised into their own table so subscriptions can reference them by ID.
                $"CREATE TABLE IF NOT EXISTS {s}.topics (\n" +
                "    id         BIGSERIAL PRIMARY KEY,\n" +
                "    topic_name TEXT      NOT NULL UNIQUE\n" +
                ");\n" +
                // Payload stored once; routing rows reference it via FK.
                $"CREATE TABLE IF NOT EXISTS {s}.message_payloads (\n" +
                "    id         BIGSERIAL PRIMARY KEY,\n" +
                "    payload    BYTEA       NOT NULL,\n" +
                "    headers    TEXT        NOT NULL,\n" +
                "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
                ");\n" +
                // One routing row per destination queue; queue_id FK replaces the old queue_name text column.
                $"CREATE TABLE IF NOT EXISTS {s}.messages (\n" +
                "    id         BIGSERIAL PRIMARY KEY,\n" +
                $"    queue_id   BIGINT      NOT NULL REFERENCES {s}.queues(id),\n" +
                $"    payload_id BIGINT      NOT NULL REFERENCES {s}.message_payloads(id),\n" +
                "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
                ");\n" +
                $"CREATE INDEX IF NOT EXISTS ix_messages_queue_pending\n" +
                $"    ON {s}.messages (queue_id, id);\n" +
                // Subscriptions now reference queues and topics by their numeric IDs.
                $"CREATE TABLE IF NOT EXISTS {s}.subscriptions (\n" +
                $"    queue_id   BIGINT NOT NULL REFERENCES {s}.queues(id),\n" +
                $"    topic_id   BIGINT NOT NULL REFERENCES {s}.topics(id),\n" +
                "    PRIMARY KEY (queue_id, topic_id)\n" +
                ");";
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Step 2: stored functions (CREATE OR REPLACE makes this idempotent).
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = BuildFunctionsSql(s);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Builds the SQL that creates or replaces all stored functions used by the transport.
    /// <para>
    /// Hot-path functions (<c>publish_to_topic</c>, <c>send_to_queue</c>, <c>consume_next</c>)
    /// use an optimistic SELECT-first strategy: they read the relevant row first and only write
    /// when the row is absent. <c>ON CONFLICT … DO NOTHING</c> handles the race where two
    /// callers create the same queue or topic simultaneously.
    /// </para>
    /// </summary>
    private static string BuildFunctionsSql(string s) =>
        // ensure_queue: returns the queue ID, creating the row only on a miss.
        $"CREATE OR REPLACE FUNCTION {s}.ensure_queue(p_queue_name TEXT)\n" +
        "RETURNS BIGINT LANGUAGE plpgsql AS $$\n" +
        "DECLARE v_queue_id BIGINT;\n" +
        "BEGIN\n" +
        $"    SELECT id INTO v_queue_id FROM {s}.queues WHERE queue_name = p_queue_name;\n" +
        "    IF FOUND THEN RETURN v_queue_id; END IF;\n" +
        $"    INSERT INTO {s}.queues (queue_name) VALUES (p_queue_name)\n" +
        "        ON CONFLICT (queue_name) DO NOTHING RETURNING id INTO v_queue_id;\n" +
        "    IF NOT FOUND THEN\n" +
        $"        SELECT id INTO v_queue_id FROM {s}.queues WHERE queue_name = p_queue_name;\n" +
        "    END IF;\n" +
        "    RETURN v_queue_id;\n" +
        "END;\n" +
        "$$;\n\n" +

        // ensure_subscription: ensures the topic row and the (queue, topic) subscription row exist.
        $"CREATE OR REPLACE FUNCTION {s}.ensure_subscription(p_queue_id BIGINT, p_topic_name TEXT)\n" +
        "RETURNS VOID LANGUAGE plpgsql AS $$\n" +
        "DECLARE v_topic_id BIGINT;\n" +
        "BEGIN\n" +
        $"    SELECT id INTO v_topic_id FROM {s}.topics WHERE topic_name = p_topic_name;\n" +
        "    IF NOT FOUND THEN\n" +
        $"        INSERT INTO {s}.topics (topic_name) VALUES (p_topic_name)\n" +
        "            ON CONFLICT (topic_name) DO NOTHING RETURNING id INTO v_topic_id;\n" +
        "        IF NOT FOUND THEN\n" +
        $"            SELECT id INTO v_topic_id FROM {s}.topics WHERE topic_name = p_topic_name;\n" +
        "        END IF;\n" +
        "    END IF;\n" +
        $"    INSERT INTO {s}.subscriptions (queue_id, topic_id) VALUES (p_queue_id, v_topic_id)\n" +
        "        ON CONFLICT (queue_id, topic_id) DO NOTHING;\n" +
        "END;\n" +
        "$$;\n\n" +

        // publish_to_topic: fan-out publish to all queues subscribed to the given topic.
        // Payload is inserted lazily — only when at least one subscriber is found — so no orphan
        // cleanup is needed for the case where the topic exists but has no subscriptions.
        $"CREATE OR REPLACE FUNCTION {s}.publish_to_topic(p_topic_name TEXT, p_payload BYTEA, p_headers TEXT)\n" +
        "RETURNS VOID LANGUAGE plpgsql AS $$\n" +
        "DECLARE\n" +
        "    v_topic_id   BIGINT;\n" +
        "    v_payload_id BIGINT;\n" +
        "    v_queue_id   BIGINT;\n" +
        "BEGIN\n" +
        // Hot path: topic is expected to exist (consumers register topics at startup).
        $"    SELECT id INTO v_topic_id FROM {s}.topics WHERE topic_name = p_topic_name;\n" +
        "    IF NOT FOUND THEN RETURN; END IF;\n" +
        $"    FOR v_queue_id IN SELECT queue_id FROM {s}.subscriptions WHERE topic_id = v_topic_id LOOP\n" +
        "        IF v_payload_id IS NULL THEN\n" +
        $"            INSERT INTO {s}.message_payloads (payload, headers) VALUES (p_payload, p_headers)\n" +
        "                RETURNING id INTO v_payload_id;\n" +
        "        END IF;\n" +
        $"        INSERT INTO {s}.messages (queue_id, payload_id) VALUES (v_queue_id, v_payload_id);\n" +
        "        PERFORM pg_notify('ib_' || v_queue_id::TEXT, '');\n" +
        "    END LOOP;\n" +
        "END;\n" +
        "$$;\n\n" +

        // send_to_queue: direct send, bypassing topic routing.
        // SELECT-first optimistic read; falls back to insert with race-condition handling.
        $"CREATE OR REPLACE FUNCTION {s}.send_to_queue(p_queue_name TEXT, p_payload BYTEA, p_headers TEXT)\n" +
        "RETURNS VOID LANGUAGE plpgsql AS $$\n" +
        "DECLARE\n" +
        "    v_queue_id   BIGINT;\n" +
        "    v_payload_id BIGINT;\n" +
        "BEGIN\n" +
        // Hot path: queue is expected to exist (registered by the consumer pump at startup).
        $"    SELECT id INTO v_queue_id FROM {s}.queues WHERE queue_name = p_queue_name;\n" +
        "    IF NOT FOUND THEN\n" +
        $"        INSERT INTO {s}.queues (queue_name) VALUES (p_queue_name)\n" +
        "            ON CONFLICT (queue_name) DO NOTHING RETURNING id INTO v_queue_id;\n" +
        "        IF NOT FOUND THEN\n" +
        $"            SELECT id INTO v_queue_id FROM {s}.queues WHERE queue_name = p_queue_name;\n" +
        "        END IF;\n" +
        "    END IF;\n" +
        $"    INSERT INTO {s}.message_payloads (payload, headers) VALUES (p_payload, p_headers)\n" +
        "        RETURNING id INTO v_payload_id;\n" +
        $"    INSERT INTO {s}.messages (queue_id, payload_id) VALUES (v_queue_id, v_payload_id);\n" +
        "    PERFORM pg_notify('ib_' || v_queue_id::TEXT, '');\n" +
        "END;\n" +
        "$$;\n\n" +

        // consume_next: locks and returns the next pending message for a queue.
        // Must be called inside an explicit transaction; the FOR UPDATE lock is held by the
        // caller's transaction (not released when the function returns).
        $"CREATE OR REPLACE FUNCTION {s}.consume_next(p_queue_id BIGINT)\n" +
        "RETURNS TABLE (message_id BIGINT, payload BYTEA, headers TEXT, payload_id BIGINT)\n" +
        "LANGUAGE plpgsql AS $$\n" +
        "BEGIN\n" +
        "    RETURN QUERY\n" +
        $"    SELECT m.id, mp.payload, mp.headers, m.payload_id\n" +
        $"    FROM {s}.messages m\n" +
        $"    JOIN {s}.message_payloads mp ON mp.id = m.payload_id\n" +
        "    WHERE m.queue_id = p_queue_id\n" +
        "    ORDER BY m.id\n" +
        "    FOR UPDATE OF m SKIP LOCKED\n" +
        "    LIMIT 1;\n" +
        "END;\n" +
        "$$;\n\n" +

        // ack_message: deletes the routing row and cleans up the payload when no other queue
        // still references it. Combines what was previously two separate round trips.
        $"CREATE OR REPLACE FUNCTION {s}.ack_message(p_message_id BIGINT, p_payload_id BIGINT)\n" +
        "RETURNS VOID LANGUAGE plpgsql AS $$\n" +
        "BEGIN\n" +
        $"    DELETE FROM {s}.messages WHERE id = p_message_id;\n" +
        $"    DELETE FROM {s}.message_payloads WHERE id = p_payload_id\n" +
        $"        AND NOT EXISTS (SELECT 1 FROM {s}.messages WHERE payload_id = p_payload_id);\n" +
        "END;\n" +
        "$$;\n\n" +

        // cleanup_orphaned_payloads: periodic sweep that removes any payload row no longer
        // referenced by any routing row. Handles the rare concurrent fan-out race described
        // in the pump's fallback-poll logic.
        $"CREATE OR REPLACE FUNCTION {s}.cleanup_orphaned_payloads()\n" +
        "RETURNS VOID LANGUAGE plpgsql AS $$\n" +
        "BEGIN\n" +
        $"    DELETE FROM {s}.message_payloads\n" +
        $"    WHERE NOT EXISTS (SELECT 1 FROM {s}.messages WHERE payload_id = message_payloads.id);\n" +
        "END;\n" +
        "$$;";

    private static string QuoteIdentifier(string name) =>
        $"\"{name.Replace("\"", "\"\"")}\"";
}
