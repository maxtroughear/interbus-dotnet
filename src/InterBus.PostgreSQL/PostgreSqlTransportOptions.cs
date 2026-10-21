using InterBus.Retry;

namespace InterBus.PostgreSQL;

/// <summary>
/// Configuration options for the PostgreSQL transport.
/// Populated via <c>AddPostgreSqlTransport(configure)</c> during startup.
/// </summary>
public sealed class PostgreSqlTransportOptions
{
    /// <summary>Additional lease-mode delivery ownership slots beyond concurrency. Defaults to zero.</summary>
    public int PrefetchCount { get; set; }

    /// <summary>The connection string used to connect to PostgreSQL.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// The PostgreSQL schema to use for the InterBus messages table.
    /// Defaults to <c>interbus</c>. The schema is created automatically if it does not exist.
    /// </summary>
    public string SchemaName { get; set; } = "interbus";

    /// <summary>
    /// Selects how the PostgreSQL transport claims in-flight messages.
    /// Defaults to <see cref="PostgreSqlMessagePumpMode.Lease"/> so long-running consumers do not
    /// hold a database connection open for the duration of message handling.
    /// </summary>
    public PostgreSqlMessagePumpMode MessagePumpMode { get; set; } = PostgreSqlMessagePumpMode.Lease;

    /// <summary>
    /// Duration for which a claimed message remains invisible to other consumers before its lease
    /// must be renewed. Defaults to 60 seconds.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Interval at which the lease-based pump renews the visibility lease for an in-flight message.
    /// When <c>null</c>, the transport renews at one-third of <see cref="LeaseDuration"/>.
    /// </summary>
    public TimeSpan? LeaseRenewInterval { get; set; }

    /// <summary>
    /// Delay before a failed leased message becomes visible again.
    /// Defaults to immediate retry to preserve the transport's current failure semantics.
    /// </summary>
    public TimeSpan FailureRedeliveryDelay { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Interval at which the message pump polls for pending messages as a fallback.
    /// This catches messages that were not processed due to consumer failures or missed
    /// notifications. Defaults to 5 seconds. Lower values increase responsiveness to
    /// failures at the cost of more frequent database queries.
    /// </summary>
    public TimeSpan FallbackPollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Configures retry behavior for PostgreSQL connection-related failures in hosted services
    /// such as schema migration and the message pump.
    /// <para>
    /// Defaults to effectively unbounded retries with exponential backoff from 1 second up to
    /// 30 seconds. Only PostgreSQL connection-related exceptions are eligible for retry.
    /// </para>
    /// </summary>
    public RetryOptions ConnectionRetry { get; set; } = new()
    {
        RetryCount = int.MaxValue,
        DelayProvider = RetryDelays.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)),
    };

    /// <summary>
    /// Whether to run schema migrations automatically at startup.
    /// Defaults to <c>true</c>. Set to <c>false</c> if you manage the database schema
    /// through an external migration tool (e.g. Flyway, Liquibase, or a custom deployment
    /// pipeline). When disabled the <c>interbus.messages</c> and <c>interbus.subscriptions</c>
    /// tables must exist before the application starts.
    /// </summary>
    public bool RunSchemaMigrations { get; set; } = true;

    internal TimeSpan GetEffectiveLeaseRenewInterval()
    {
        if (LeaseRenewInterval is { } configured)
            return configured;

        var interval = TimeSpan.FromTicks(LeaseDuration.Ticks / 3);
        return interval > TimeSpan.Zero ? interval : TimeSpan.FromSeconds(1);
    }
}
