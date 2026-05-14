namespace InterBus.EntityFrameworkCore.Outbox;

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
    /// processor is allowed to pick it up. Defaults to <c>5 seconds</c> as a safe value that
    /// reduces the risk of the processor racing with inline delivery (registered via
    /// <c>UseOutboxWithInlineDelivery&lt;TDbContext&gt;()</c>). If inline delivery succeeds and
    /// deletes the group within this window, the processor never sees the entry. If inline
    /// delivery fails, the processor retries after the timeout elapses.
    /// <para>
    /// Set to <see cref="TimeSpan.Zero"/> to make consumer-published groups immediately eligible
    /// for the background processor (useful when inline delivery is not configured).
    /// </para>
    /// </summary>
    public TimeSpan ConsumerVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public bool RunOutboxProcessor { get; internal set; } = true;
}
