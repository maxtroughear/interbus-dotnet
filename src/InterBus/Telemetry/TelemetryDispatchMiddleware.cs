using System.Diagnostics;
using InterBus.DeadLettering;
using InterBus.Dispatching;

namespace InterBus.Telemetry;

/// <summary>
/// Dispatch middleware that starts the consumer <see cref="Activity"/>, records consumed/failed
/// metrics, sets the activity status, and disposes the activity on completion.
/// Registered as the outermost dispatch middleware so it wraps the entire message processing span.
/// </summary>
public sealed class TelemetryDispatchMiddleware(
    IInterBusDiagnostics diagnostics,
    InterBusMetrics metrics) : IDispatchMiddleware
{
    public async Task InvokeAsync(
        DispatchContext context,
        Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        var consumerName = context.Descriptor.ConsumerType.Name;
        var queueName = context.Descriptor.QueueName;
        var activity = InterBusTelemetry.StartConsumerActivity(diagnostics, context.Headers, consumerName, queueName);
        try
        {
            await next(context, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
            metrics.RecordConsumed(consumerName, queueName);
        }
        catch (DeadLetterException ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Reason);
            metrics.RecordFailed(consumerName, queueName);
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
        finally
        {
            activity?.Dispose();
        }
    }
}
