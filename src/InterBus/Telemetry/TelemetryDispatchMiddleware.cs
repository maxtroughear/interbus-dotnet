using System.Diagnostics;
using InterBus.Dispatching;

namespace InterBus.Telemetry;

/// <summary>
/// Dispatch middleware that creates a consumer <see cref="Activity"/> for each incoming message,
/// propagates W3C trace context from the message headers, and records consume/fail counters.
/// Registered as the outermost dispatch middleware so it wraps the entire message processing span
/// (deserialization + consume pipeline).
/// </summary>
public sealed class TelemetryDispatchMiddleware(IInterBusDiagnostics diagnostics, InterBusMetrics metrics)
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
            metrics.MessagesConsumed.Add(1,
                new TagList
                {
                    { "messaging.system", "interbus" },
                    { "messaging.consumer.name", consumerName },
                    { "messaging.destination.name", context.Descriptor.QueueName },
                });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            metrics.MessagesFailed.Add(1,
                new TagList
                {
                    { "messaging.system", "interbus" },
                    { "messaging.consumer.name", consumerName },
                    { "messaging.destination.name", context.Descriptor.QueueName },
                });
            throw;
        }
    }

    private Activity? StartActivity(DispatchContext context, string consumerName, string? messageType)
    {
        ActivityContext parentContext = default;
        var hasParent = context.Headers.TryGetValue(InterBusHeaders.Traceparent, out var traceparent)
            && ActivityContext.TryParse(traceparent, context.Headers.GetValueOrDefault(InterBusHeaders.Tracestate),
                isRemote: true, out parentContext);

        var activityName = $"{consumerName} process";
        var activity = hasParent
            ? diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Consumer, parentContext)
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
