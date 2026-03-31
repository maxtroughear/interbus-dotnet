using TickerQ.Utilities.Base;

namespace InterBus.TickerQ.Internal;

/// <summary>
/// TickerQ function handler for scheduled queue-send messages (point-to-point).
/// Resolved from DI via constructor injection; called by TickerQ when the scheduled job fires.
/// </summary>
internal sealed class TickerQSendFunction(IInterBusTransport transport)
{
    /// <summary>
    /// TickerQ function name registered for scheduled send (point-to-point) messages.
    /// </summary>
    public const string FunctionName = "interbus-send";

    /// <summary>
    /// Called by TickerQ when a scheduled send job fires. The typed context carries
    /// the <see cref="ScheduledSendRequest"/> deserialized from the persisted request bytes.
    /// </summary>
    public Task InvokeAsync(TickerFunctionContext<ScheduledSendRequest> context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var headers = new MessageHeaders(request.Headers);
        return transport.SendToQueueAsync(request.QueueName, request.Payload, headers, cancellationToken);
    }
}
