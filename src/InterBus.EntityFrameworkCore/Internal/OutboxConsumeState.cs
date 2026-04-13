namespace InterBus.EntityFrameworkCore.Internal;

/// <summary>
/// Scoped state shared between <see cref="OutboxPublishMiddleware{TDbContext}"/> and
/// <see cref="OutboxConsumeMiddleware{TDbContext}"/> within a single message-dispatch scope.
/// <para>
/// <see cref="OutboxPublishMiddleware{TDbContext}"/> sets <see cref="GroupId"/> when it writes an
/// outbox group, so that the paired consume middleware can immediately deliver that
/// group after the consumer's surrounding transaction has committed.
/// </para>
/// </summary>
internal sealed class OutboxConsumeState
{
    /// <summary>
    /// The <see cref="OutboxGroup.Id"/> written by the publish middleware during this dispatch.
    /// <c>null</c> if no outbox group was written in this scope
    /// </summary>
    public Guid? GroupId { get; internal set; }
}
