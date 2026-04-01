using TickerQ.Utilities.Base;

namespace InterBus.TickerQ.Internal;

/// <summary>
/// TickerQ function handler for all scheduled InterBus messages (both topic fan-out and
/// point-to-point queue send). Resolved from DI via constructor injection; called by TickerQ
/// when the scheduled job fires.
/// </summary>
internal sealed class TickerQDeliverFunction(IInterBusTransport transport)
{
    /// <summary>
    /// TickerQ function name registered for all scheduled InterBus message delivery.
    /// </summary>
    public const string FunctionName = "interbus-deliver";

    /// <summary>
    /// Called by TickerQ when a scheduled delivery job fires. The typed context carries
    /// the <see cref="ScheduledDeliveryRequest"/> deserialized from the persisted request bytes.
    /// </summary>
    public Task InvokeAsync(TickerFunctionContext<ScheduledDeliveryRequest> context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var headers = new MessageHeaders(request.Headers);
        return transport.SendAsync(new Uri(request.Destination), request.Payload, headers, cancellationToken);
    }
}
