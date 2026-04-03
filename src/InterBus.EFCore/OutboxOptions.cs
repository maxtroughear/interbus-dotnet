namespace InterBus.EFCore;

/// <summary>
/// Configuration options for the InterBus transactional outbox.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// How often the background processor polls the outbox table for pending messages.
    /// Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of
    /// more frequent database queries.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of messages to process in a single polling iteration.
    /// Defaults to 100.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// When <c>true</c>, successfully processed messages are retained in the outbox table
    /// with <see cref="OutboxMessage.ProcessedAt"/> set. When <c>false</c> (the default),
    /// processed messages are deleted to keep the table small.
    /// </summary>
    public bool KeepProcessedMessages { get; set; }
}
