namespace InterBus.EntityFrameworkCore.Outbox;

/// <summary>
/// Scoped state shared between <see cref="OutboxPublishMiddleware{TDbContext}"/> and
/// <see cref="InboxOutboxConsumeMiddleware{TDbContext}"/> within a single message-dispatch scope.
/// </summary>
internal sealed class OutboxConsumeState
{
    public InboxState? Inbox { get; internal set; }
}
