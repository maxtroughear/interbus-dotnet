namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Scoped state shared between <see cref="OutboxPublishMiddleware{TDbContext}"/> and
/// <see cref="InboxOutboxConsumeMiddleware{TDbContext}"/> within a single message-dispatch scope.
/// <para>
/// <see cref="OutboxPublishMiddleware{TDbContext}"/> sets <see cref="GroupId"/> when it writes an
/// outbox group, so that the paired consume middleware can immediately deliver that
/// group after the consumer's surrounding transaction has committed.
/// </para>
/// </summary>
internal sealed class OutboxConsumeState
{
    public InboxState? Inbox { get; internal set; }
}
