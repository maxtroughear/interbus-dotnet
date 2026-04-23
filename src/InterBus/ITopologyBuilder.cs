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
            "Topic topology must be implemented by the transport-specific topology builder.");

    /// <summary>
    /// Ensure a send queue exists (or is verified) and return its transport-specific identifier.
    /// </summary>
    Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken) =>
        throw new NotSupportedException(
            "Queue topology must be implemented by the transport-specific topology builder.");
}
