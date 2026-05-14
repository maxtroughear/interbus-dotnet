namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Scoped state shared between <see cref="DynamoDbOutboxPublishMiddleware"/> and
/// <see cref="DynamoDbOutboxInlineDeliveryConsumeMiddleware"/> within a single consumer dispatch
/// scope.
/// </summary>
internal sealed class DynamoDbOutboxConsumeState
{
    /// <summary>
    /// The GroupId written by the outbox context during <c>CommitAsync</c> in this dispatch.
    /// <c>null</c> if no outbox group was committed in this scope.
    /// </summary>
    public Guid? GroupId { get; internal set; }

    /// <summary>
    /// When <c>true</c>, the outbox context applies the consumer visibility timeout so that
    /// the background processor does not race with inline delivery.
    /// Set by <see cref="DynamoDbOutboxInlineDeliveryConsumeMiddleware"/> before the consumer
    /// handler runs.
    /// </summary>
    public bool IsDeliveryInline { get; internal set; }
}
