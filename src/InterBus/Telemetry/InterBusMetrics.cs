using System.Diagnostics.Metrics;

namespace InterBus.Telemetry;

/// <summary>
/// Internal singleton that owns the InterBus <see cref="Meter"/> and its counters.
/// </summary>
public sealed class InterBusMetrics : IDisposable
{
    private readonly Meter _meter = new(InterBusTelemetry.MeterName);

    /// <summary>Number of messages published or sent.</summary>
    internal Counter<long> MessagesPublished { get; }

    /// <summary>Number of messages successfully processed by a consumer.</summary>
    internal Counter<long> MessagesConsumed { get; }

    /// <summary>Number of messages that threw an unhandled exception during consumption.</summary>
    internal Counter<long> MessagesFailed { get; }

    public InterBusMetrics()
    {
        MessagesPublished = _meter.CreateCounter<long>(
            "interbus.messages.published",
            unit: "{message}",
            description: "Number of messages published by InterBus.");

        MessagesConsumed = _meter.CreateCounter<long>(
            "interbus.messages.consumed",
            unit: "{message}",
            description: "Number of messages successfully consumed by InterBus.");

        MessagesFailed = _meter.CreateCounter<long>(
            "interbus.messages.failed",
            unit: "{message}",
            description: "Number of messages that failed during consumption by InterBus.");
    }

    public void Dispose() => _meter.Dispose();
}
