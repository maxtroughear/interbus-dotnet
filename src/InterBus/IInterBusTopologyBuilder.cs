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
}
