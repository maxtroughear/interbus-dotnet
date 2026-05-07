using System.Diagnostics;

namespace InterBus.Telemetry;

/// <summary>
/// Exposes the InterBus <see cref="ActivitySource"/> for distributed tracing.
/// Register the source name (<see cref="InterBusTelemetry.ActivitySourceName"/>) with your
/// OpenTelemetry SDK to receive InterBus traces.
/// </summary>
public interface IInterBusDiagnostics
{
    /// <summary>The <see cref="ActivitySource"/> used by all InterBus activities.</summary>
    ActivitySource ActivitySource { get; }
}
