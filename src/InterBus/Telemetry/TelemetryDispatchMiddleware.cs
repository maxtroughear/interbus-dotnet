using System.Diagnostics;
using InterBus.Dispatching;

namespace InterBus.Telemetry;

/// <summary>
/// Dispatch middleware that creates a consumer <see cref="Activity"/> for each incoming message
/// and marks it OK or Error based on the outcome of the dispatch pipeline.
/// Registered as the outermost dispatch middleware so it wraps the entire message processing span
/// (deserialization + consume pipeline).
/// <para>
/// The parent trace context must be extracted from the incoming message headers by the message
/// pump (via <see cref="InterBusTelemetry.TryExtractParentContext"/>) and stored on
/// <see cref="DispatchContext.RemoteParentContext"/> before reaching this middleware.
/// </para>
/// </summary>
public sealed class TelemetryDispatchMiddleware(IInterBusDiagnostics diagnostics)
    : IDispatchMiddleware
{
    public async Task InvokeAsync(
        DispatchContext context,
        Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        var messageType = context.Headers.GetValueOrDefault(InterBusHeaders.MessageType);
        var consumerName = context.Descriptor.ConsumerType.Name;

        using var activity = StartActivity(context, consumerName, messageType);

        try
        {
            await next(context, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    private Activity? StartActivity(DispatchContext context, string consumerName, string? messageType)
    {
        var activityName = $"{consumerName} process";
        var activity = context.RemoteParentContext.HasValue
            ? diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Consumer,
                context.RemoteParentContext.Value)
            : diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Consumer);

        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "interbus");
        activity.SetTag("messaging.operation.type", "process");
        activity.SetTag("messaging.destination.name", context.Descriptor.QueueName);
        activity.SetTag("messaging.consumer.name", consumerName);

        if (messageType is not null)
            activity.SetTag("messaging.message.type", messageType);

        if (context.Headers.TryGetValue(InterBusHeaders.MessageId, out var messageId))
            activity.SetTag("messaging.message.id", messageId);

        if (context.Headers.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId))
            activity.SetTag("messaging.message.correlation_id", correlationId);

        return activity;
    }
}
