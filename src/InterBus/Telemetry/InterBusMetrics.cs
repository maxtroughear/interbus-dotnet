using System.Diagnostics;
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

    /// <summary>Number of messages that were dead-lettered during consumption.</summary>
    internal Counter<long> MessagesFailed { get; }

    /// <summary>Duration of the full consumer dispatch pipeline execution.</summary>
    internal Histogram<double> ConsumerDuration { get; }

    /// <summary>Number of request/reply exchanges started by request clients.</summary>
    internal Counter<long> RequestsStarted { get; }

    /// <summary>Number of request/reply exchanges that completed with a reply.</summary>
    internal Counter<long> RequestsCompleted { get; }

    /// <summary>Number of request/reply exchanges that failed.</summary>
    internal Counter<long> RequestsFailed { get; }

    /// <summary>Duration of a request/reply exchange from publish to reply receive.</summary>
    internal Histogram<double> RequestDuration { get; }

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
            description: "Number of messages that were dead-lettered during consumption by InterBus.");

        ConsumerDuration = _meter.CreateHistogram<double>(
            "interbus.consumer.duration",
            unit: "s",
            description: "Duration of the InterBus consumer dispatch pipeline execution in seconds.");

        RequestsStarted = _meter.CreateCounter<long>(
            "interbus.requests.started",
            unit: "{request}",
            description: "Number of request/reply exchanges started by InterBus request clients.");

        RequestsCompleted = _meter.CreateCounter<long>(
            "interbus.requests.completed",
            unit: "{request}",
            description: "Number of request/reply exchanges completed by InterBus request clients.");

        RequestsFailed = _meter.CreateCounter<long>(
            "interbus.requests.failed",
            unit: "{request}",
            description: "Number of failed request/reply exchanges in InterBus request clients.");

        RequestDuration = _meter.CreateHistogram<double>(
            "interbus.request.duration",
            unit: "s",
            description: "Duration of InterBus request/reply exchanges in seconds.");
    }

    /// <summary>
    /// Increments the <c>interbus.messages.consumed</c> counter.
    /// Called by <see cref="TelemetryDispatchMiddleware"/> after the dispatch pipeline completes successfully.
    /// </summary>
    public void RecordConsumed(string consumerName, string queueName) =>
        MessagesConsumed.Add(1, new TagList
        {
            { "messaging.system", "interbus" },
            { "messaging.consumer.name", consumerName },
            { "messaging.destination.name", queueName },
        });

    /// <summary>
    /// Increments the <c>interbus.messages.failed</c> counter.
    /// Called by <see cref="TelemetryDispatchMiddleware"/> when the dispatch pipeline requests dead-lettering.
    /// </summary>
    public void RecordFailed(string consumerName, string queueName) =>
        MessagesFailed.Add(1, new TagList
        {
            { "messaging.system", "interbus" },
            { "messaging.consumer.name", consumerName },
            { "messaging.destination.name", queueName },
        });

    /// <summary>
    /// Records the duration of the consumer dispatch pipeline.
    /// Called by <see cref="TelemetryDispatchMiddleware"/> after the pipeline completes (success or failure).
    /// </summary>
    public void RecordDuration(double seconds, string consumerName, string queueName) =>
        ConsumerDuration.Record(seconds, new TagList
        {
            { "messaging.system", "interbus" },
            { "messaging.consumer.name", consumerName },
            { "messaging.destination.name", queueName },
        });

    public void RecordRequestStarted(string requestType, string replyType, string destinationName) =>
        RequestsStarted.Add(1, new TagList
        {
            { "messaging.system", "interbus" },
            { "messaging.operation.type", "request" },
            { "messaging.destination.name", destinationName },
            { "messaging.message.type", requestType },
            { "messaging.message.reply_type", replyType },
        });

    public void RecordRequestCompleted(string requestType, string replyType, string destinationName) =>
        RequestsCompleted.Add(1, new TagList
        {
            { "messaging.system", "interbus" },
            { "messaging.operation.type", "request" },
            { "messaging.destination.name", destinationName },
            { "messaging.message.type", requestType },
            { "messaging.message.reply_type", replyType },
        });

    public void RecordRequestFailed(string requestType, string replyType, string destinationName) =>
        RequestsFailed.Add(1, new TagList
        {
            { "messaging.system", "interbus" },
            { "messaging.operation.type", "request" },
            { "messaging.destination.name", destinationName },
            { "messaging.message.type", requestType },
            { "messaging.message.reply_type", replyType },
        });

    public void RecordRequestDuration(double seconds, string requestType, string replyType, string destinationName) =>
        RequestDuration.Record(seconds, new TagList
        {
            { "messaging.system", "interbus" },
            { "messaging.operation.type", "request" },
            { "messaging.destination.name", destinationName },
            { "messaging.message.type", requestType },
            { "messaging.message.reply_type", replyType },
        });

    public void Dispose() => _meter.Dispose();
}
