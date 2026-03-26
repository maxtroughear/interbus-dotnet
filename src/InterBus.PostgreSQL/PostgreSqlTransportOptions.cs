namespace InterBus.PostgreSQL;

/// <summary>
/// Configuration options for the PostgreSQL transport.
/// Populated via <c>AddPostgreSqlTransport(configure)</c> during startup.
/// </summary>
public sealed class PostgreSqlTransportOptions
{
    /// <summary>The connection string used to connect to PostgreSQL.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// The PostgreSQL schema to use for the InterBus messages table.
    /// Defaults to <c>interbus</c>. The schema is created automatically if it does not exist.
    /// </summary>
    public string SchemaName { get; set; } = "interbus";

    /// <summary>
    /// Interval at which the message pump polls for pending messages as a fallback.
    /// This catches messages that were not processed due to consumer failures or missed
    /// notifications. Defaults to 30 seconds. Lower values increase responsiveness to
    /// failures at the cost of more frequent database queries.
    /// </summary>
    public TimeSpan FallbackPollInterval { get; set; } = TimeSpan.FromSeconds(30);
}
