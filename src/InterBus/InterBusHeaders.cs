namespace InterBus;

/// <summary>
/// Well-known header keys set by InterBus internally.
/// All InterBus-managed headers are prefixed with <c>interbus-</c> to avoid collisions
/// with user-defined headers.
/// </summary>
public static class InterBusHeaders
{
    /// <summary>Header key for the correlation identifier.</summary>
    public const string CorrelationId = "interbus-correlation-id";
}
