using TickerQ.Utilities.Base;

namespace InterBus.TickerQ.Internal;

/// <summary>
/// TickerQ function handler for scheduled topic-publish messages (fan-out).
/// Resolved from DI via constructor injection; called by TickerQ when the scheduled job fires.
/// </summary>
internal sealed class TickerQPublishFunction(IInterBusTransport transport)
{
    /// <summary>
    /// TickerQ function name registered for scheduled publish (topic fan-out) messages.
    /// </summary>
    public const string FunctionName = "interbus-publish";

    /// <summary>
    /// Called by TickerQ when a scheduled publish job fires. The typed context carries
    /// the <see cref="ScheduledPublishRequest"/> deserialized from the persisted request bytes.
    /// </summary>
    public Task InvokeAsync(TickerFunctionContext<ScheduledPublishRequest> context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var headers = new MessageHeaders(request.Headers);
        return transport.PublishToTopicAsync(request.TopicName, request.Payload, headers, cancellationToken);
    }
}
