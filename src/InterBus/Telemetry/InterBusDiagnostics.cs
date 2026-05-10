using System.Diagnostics;

namespace InterBus.Telemetry;

/// <summary>
/// Internal singleton that owns the <see cref="System.Diagnostics.ActivitySource"/> used
/// across the InterBus library.
/// </summary>
internal sealed class InterBusDiagnostics : IInterBusDiagnostics, IDisposable
{
    public ActivitySource ActivitySource { get; } = new(InterBusTelemetry.ActivitySourceName);

    public void Dispose() => ActivitySource.Dispose();
}
