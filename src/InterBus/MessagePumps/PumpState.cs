namespace InterBus.MessagePumps;

/// <summary>Lifecycle state of a supervised consumer runtime.</summary>
public enum PumpState
{
    /// <summary>Initialization has not begun.</summary>
    NotStarted,
    /// <summary>All work has stopped and adapter resources have been disposed.</summary>
    Stopped,
    /// <summary>Transport resources are initializing.</summary>
    Starting,
    /// <summary>Intake and processing are active.</summary>
    Running,
    /// <summary>An unhandled failure was reported. Accepted work may still be draining.</summary>
    Faulted,
    /// <summary>Intake is stopping and accepted work is draining.</summary>
    Stopping
}
