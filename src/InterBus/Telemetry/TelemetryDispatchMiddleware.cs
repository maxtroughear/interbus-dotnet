using System.Diagnostics;
using InterBus.Dispatching;

namespace InterBus.Telemetry;

/// <summary>
/// Dispatch middleware that marks the ambient consumer <see cref="Activity"/> (started by the
/// message pump via <see cref="InterBusTelemetry.StartConsumerActivity"/>) as Error when the
/// dispatch pipeline throws an unhandled exception.
/// Registered as the outermost dispatch middleware so it wraps the entire message processing span.
/// </summary>
public sealed class TelemetryDispatchMiddleware : IDispatchMiddleware
{
    public async Task InvokeAsync(
        DispatchContext context,
        Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await next(context, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Activity.Current?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
}
