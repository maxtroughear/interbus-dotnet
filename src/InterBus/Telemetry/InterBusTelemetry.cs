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
    /// Extracts the W3C distributed trace parent context from InterBus message headers.
    /// Called by each message pump before dispatching so that the consumer activity is
    /// created with the correct remote parent without re-parsing headers inside middleware.
    /// </summary>
    /// <param name="headers">Incoming message headers.</param>
    /// <param name="parentContext">
    /// The parsed <see cref="ActivityContext"/> when the method returns <c>true</c>;
    /// otherwise <c>default</c>.
    /// </param>
    /// <returns><c>true</c> if a valid <c>traceparent</c> header was found and parsed.</returns>
    public static bool TryExtractParentContext(MessageHeaders headers, out ActivityContext parentContext)
    {
        if (headers.TryGetValue(InterBusHeaders.Traceparent, out var traceparent)
            && ActivityContext.TryParse(traceparent, headers.GetValueOrDefault(InterBusHeaders.Tracestate),
                isRemote: true, out parentContext))
            return true;

        parentContext = default;
        return false;
    }
}
