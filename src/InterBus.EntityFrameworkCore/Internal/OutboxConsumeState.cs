namespace InterBus.EntityFrameworkCore.Internal;

/// <summary>
/// Scoped state shared between <see cref="OutboxPublishMiddleware{TDbContext}"/> and
/// <see cref="OutboxConsumeMiddleware{TDbContext}"/> within a single message-dispatch scope.
/// <para>
/// <see cref="OutboxPublishMiddleware{TDbContext}"/> sets <see cref="OutboxGroup"/> when it writes an
/// outbox group, so that the paired consume middleware can immediately deliver that
/// group after the consumer's surrounding transaction has committed.
/// </para>
/// </summary>
internal sealed class OutboxConsumeState
{
    /// <summary>
    /// The outbox group that was written by the publish middleware during this dispatch.
    /// </summary>
    public OutboxGroup? OutboxGroup { get; set; }
}
