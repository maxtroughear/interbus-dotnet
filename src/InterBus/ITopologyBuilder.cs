namespace InterBus;

/// <summary>
/// Transport-specific topology builder invoked by the bus during startup.
/// Implementations create or verify the transport entities required by registered consumers.
/// </summary>
public interface ITopologyBuilder
{
    /// <summary>
    /// Build or verify transport topology for registered consumers.
    /// </summary>
    Task BuildTopologyAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Ensure a publish topic exists (or is verified) and return its transport-specific identifier.
    /// </summary>
    Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken) =>
        throw new NotSupportedException(
            "Topic topology must be implemented by the transport-specific topology builder");

    /// <summary>
    /// Ensure a send queue exists (or is verified) and return its transport-specific identifier
    /// (e.g. a queue URL for SQS).
    /// </summary>
    Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken) =>
        throw new NotSupportedException(
            "Queue topology must be implemented by the transport-specific topology builder");

    /// <summary>
    /// Ensure a send queue exists (or is verified) and return its ARN or equivalent
    /// resource identifier required when targeting the queue from an external service
    /// (e.g. EventBridge Scheduler). The default implementation delegates to
    /// <see cref="EnsureQueueAsync"/> and is suitable for transports where the queue URL
    /// and resource ARN are the same. Transports that distinguish between URLs and ARNs
    /// (e.g. SQS) should override this method to return the ARN.
    /// </summary>
    Task<string> EnsureQueueArnAsync(string rawQueueName, CancellationToken cancellationToken) =>
        EnsureQueueAsync(rawQueueName, cancellationToken);
}
