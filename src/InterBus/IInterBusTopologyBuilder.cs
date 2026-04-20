namespace InterBus;

/// <summary>
/// Transport-specific topology builder invoked by the bus during startup.
/// Implementations create or verify the transport entities required by registered consumers.
/// </summary>
public interface IInterBusTopologyBuilder
{
    /// <summary>
    /// Build or verify transport topology for registered consumers.
    /// </summary>
    Task BuildTopologyAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Ensure a publish topic exists (or is verified) and return its transport-specific identifier.
    /// </summary>
    Task<string> EnsureTopicAsync(string rawTopicName, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This transport does not support topic topology.");

    /// <summary>
    /// Ensure a send queue exists (or is verified) and return its transport-specific identifier.
    /// </summary>
    Task<string> EnsureQueueAsync(string rawQueueName, CancellationToken cancellationToken) =>
        throw new NotSupportedException("This transport does not support queue topology.");
}
