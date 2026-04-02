using TickerQ.Utilities.Base;

namespace InterBus.TickerQ.Internal;

/// <summary>
/// TickerQ function handler for all scheduled InterBus messages (both topic fan-out and
/// point-to-point queue send). Resolved from DI via constructor injection; called by TickerQ
/// when the scheduled job fires.
/// </summary>
internal sealed class TickerQDeliverFunction(IInterBusTransportFactory transportFactory)
{
    /// <summary>
    /// TickerQ function name registered for all scheduled InterBus message delivery.
    /// </summary>
    public const string FunctionName = "interbus-deliver";

    /// <summary>
    /// Called by TickerQ when a scheduled delivery job fires. The typed context carries
    /// the <see cref="ScheduledDeliveryRequest"/> deserialized from the persisted request bytes.
    /// The correct transport is resolved from the destination URI scheme via
    /// <see cref="IInterBusTransportFactory"/>, matching the same factory used by
    /// <c>MessagePublisher</c> at publish time.
    /// </summary>
    public Task InvokeAsync(TickerFunctionContext<ScheduledDeliveryRequest> context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var destination = new Uri(request.Destination);
        var headers = new MessageHeaders(request.Headers);
        var transport = transportFactory.Resolve(destination);
        var sendContext = new TransportSendContext(destination, request.Payload, headers, delay: null);
        return transport.SendAsync(sendContext, cancellationToken);
    }
}
