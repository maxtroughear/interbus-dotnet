namespace InterBus.Telemetry;

/// <summary>
/// Well-known telemetry names for use when registering OpenTelemetry instrumentation.
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
    /// <summary>The name of the InterBus <see cref="System.Diagnostics.ActivitySource"/>.</summary>
    public const string ActivitySourceName = "InterBus";

    /// <summary>The name of the InterBus <see cref="System.Diagnostics.Metrics.Meter"/>.</summary>
    public const string MeterName = "InterBus";
}
