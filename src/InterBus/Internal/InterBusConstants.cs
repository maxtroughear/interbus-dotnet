namespace InterBus.Internal;

internal static class InterBusConstants
{
    public const string BusContext = "InterBusBusContext";

    /// <summary>
    /// Keyed service key used to register middlewares that apply to every consumer's
    /// publish, send, or schedule pipeline (in addition to any consumer-specific middlewares).
    /// </summary>
    public const string AllConsumersContext = "InterBusAllConsumersContext";
}
