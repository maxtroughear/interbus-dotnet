namespace InterBus.DynamoDB.Outbox;

/// <summary>
/// Scoped state shared within a single consumer dispatch
/// scope.
/// </summary>
internal sealed class DynamoDbOutboxConsumeState
{
    /// <summary>
    /// The GroupId written by the outbox context during <c>CommitAsync</c> in this dispatch.
    /// <c>null</c> if no outbox group was committed in this scope.
    /// </summary>
    public Guid? GroupId { get; internal set; }
}
