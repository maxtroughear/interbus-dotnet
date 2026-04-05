namespace InterBus.PostgreSQL;

/// <summary>
/// Selects how the PostgreSQL transport holds ownership of in-flight messages.
/// </summary>
public enum PostgreSqlMessagePumpMode
{
    /// <summary>
    /// Claims a message by setting a renewable visibility lease on the row.
    /// This is the default mode and releases the processing connection while the
    /// consumer runs.
    /// </summary>
    Lease = 0,

    /// <summary>
    /// Holds ownership using a database transaction and <c>SELECT ... FOR UPDATE SKIP LOCKED</c>
    /// for the entire duration of message dispatch.
    /// </summary>
    Transactional = 1,
}
