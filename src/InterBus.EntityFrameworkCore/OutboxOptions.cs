namespace InterBus.EntityFrameworkCore;

/// <summary>
/// Configuration options for the InterBus transactional outbox.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// How often the background processor polls the outbox table when there are no pending
    /// messages. Defaults to 5 seconds. Lower values reduce end-to-end latency at the cost of
    /// more frequent database queries when the outbox is empty.
    /// <para>
    /// The processor does not wait between iterations when messages are available — it
    /// immediately processes the next group after completing the current one.
    /// </para>
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long after a group is created by a consumer's consume handler before the outbox
    /// processor is allowed to pick it up. Defaults to <see cref="TimeSpan.Zero"/> (immediately
    /// eligible). Increase this to allow the consumer's surrounding transaction to commit before
    /// the processor attempts to lock the group, reducing contention in high-throughput scenarios.
    /// </summary>
    public TimeSpan ConsumerVisibilityTimeout { get; set; } = TimeSpan.Zero;

    public bool RunOutboxProcessor { get; internal set; } = true;
}
