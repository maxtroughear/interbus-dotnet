using System.Diagnostics;
using InterBus.Publishing;

namespace InterBus.Telemetry;

/// <summary>
/// Publish middleware that creates a producer <see cref="Activity"/> for each outgoing message,
/// injects W3C trace context into the message headers, and records publish counters.
/// Registered as the outermost publish middleware so it wraps the entire publish span.
/// </summary>
public sealed class TelemetryPublishMiddleware(IInterBusDiagnostics diagnostics, InterBusMetrics metrics)
    : IPublishMiddleware
{
    public async Task InvokeAsync<T>(
        PublishContext<T> context,
        Func<PublishContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        var messageType = context.Headers.GetValueOrDefault(InterBusHeaders.MessageType);
        var destinationName = RoutingConventions.GetDestinationName(context.Destination);

        using var activity = StartActivity(context, messageType, destinationName);

        // Propagate trace context into outgoing message headers so consumers can link spans.
        if (activity is not null)
        {
            if (activity.Id is not null)
                context.Headers[InterBusHeaders.Traceparent] = activity.Id;
            if (!string.IsNullOrEmpty(activity.TraceStateString))
                context.Headers[InterBusHeaders.Tracestate] = activity.TraceStateString;
        }

        await next(context, cancellationToken);

        metrics.MessagesPublished.Add(1,
            new TagList
            {
                { "messaging.system", "interbus" },
                { "messaging.destination.name", destinationName },
            });
    }

    private Activity? StartActivity<T>(PublishContext<T> context, string? messageType, string destinationName)
        where T : class
    {
        var activityName = messageType is not null ? $"{messageType} publish" : "publish";
        var activity = diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Producer);

        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "interbus");
        activity.SetTag("messaging.operation.type", "publish");
        activity.SetTag("messaging.destination.name", destinationName);

        if (messageType is not null)
            activity.SetTag("messaging.message.type", messageType);

        if (context.Headers.TryGetValue(InterBusHeaders.MessageId, out var messageId))
            activity.SetTag("messaging.message.id", messageId);

        if (context.Headers.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId))
            activity.SetTag("messaging.message.correlation_id", correlationId);

        return activity;
    }
}
