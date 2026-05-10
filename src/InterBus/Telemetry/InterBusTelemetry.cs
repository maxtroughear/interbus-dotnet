using System.Diagnostics;

namespace InterBus.Telemetry;

/// <summary>
/// Well-known telemetry names for use when registering OpenTelemetry instrumentation,
/// and a helper for propagating W3C trace context from incoming message headers.
/// </summary>
/// <example>
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithTracing(tracing =&gt; tracing.AddSource(InterBusTelemetry.ActivitySourceName))
///     .WithMetrics(metrics =&gt; metrics.AddMeter(InterBusTelemetry.MeterName));
/// </code>
/// </example>
public static class InterBusTelemetry
{
    /// <summary>The name of the InterBus <see cref="ActivitySource"/>.</summary>
    public const string ActivitySourceName = "InterBus";

    /// <summary>The name of the InterBus <see cref="System.Diagnostics.Metrics.Meter"/>.</summary>
    public const string MeterName = "InterBus";

    /// <summary>
    /// Extracts the W3C trace parent from incoming message headers, starts a consumer
    /// <see cref="Activity"/> and sets the standard messaging semantic-convention tags.
    /// Called by <see cref="TelemetryDispatchMiddleware"/> before invoking the next middleware.
    /// </summary>
    /// <param name="diagnostics">The InterBus <see cref="IInterBusDiagnostics"/> singleton.</param>
    /// <param name="headers">Incoming message headers.</param>
    /// <param name="consumerName">Consumer type name, used as the activity operation name suffix.</param>
    /// <param name="queueName">Name of the queue being consumed.</param>
    /// <returns>
    /// The started <see cref="Activity"/>, or <c>null</c> when no listener has subscribed to the source.
    /// </returns>
    public static Activity? StartConsumerActivity(
        IInterBusDiagnostics diagnostics,
        MessageHeaders headers,
        string consumerName,
        string queueName)
    {
        var hasParent = headers.TryGetValue(InterBusHeaders.Traceparent, out var traceparent)
            && ActivityContext.TryParse(traceparent, headers.GetValueOrDefault(InterBusHeaders.Tracestate),
                isRemote: true, out var parentContext);

        var activityName = $"{consumerName} process";
        var activity = hasParent
            ? diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Consumer, parentContext)
            : diagnostics.ActivitySource.StartActivity(activityName, ActivityKind.Consumer);

        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "interbus");
        activity.SetTag("messaging.operation.type", "process");
        activity.SetTag("messaging.destination.name", queueName);
        activity.SetTag("messaging.consumer.name", consumerName);

        if (headers.TryGetValue(InterBusHeaders.MessageType, out var messageType))
            activity.SetTag("messaging.message.type", messageType);

        if (headers.TryGetValue(InterBusHeaders.MessageId, out var messageId))
            activity.SetTag("messaging.message.id", messageId);

        if (headers.TryGetValue(InterBusHeaders.CorrelationId, out var correlationId))
            activity.SetTag("messaging.message.correlation_id", correlationId);

        return activity;
    }
}
