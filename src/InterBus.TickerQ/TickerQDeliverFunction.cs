using InterBus.Sending;
using TickerQ.Utilities.Base;
using TickerQ.Utilities.Interfaces;

namespace InterBus.TickerQ;

/// <summary>
/// TickerQ function handler for all scheduled InterBus messages (both topic fan-out and
/// point-to-point queue send). Resolved from DI via constructor injection; called by TickerQ
/// when the scheduled job fires.
/// </summary>
internal sealed class TickerQDeliverFunction(SendPipelineExecutor sendPipeline)
    : ITickerFunction<ScheduledDeliveryRequest>
{
    public Task ExecuteAsync(TickerFunctionContext<ScheduledDeliveryRequest> context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var destination = new Uri(request.Destination);
        var headers = new MessageHeaders(request.Headers);
        var scheduleContext = new SendContext(destination, request.Payload, headers, delay: null);
        return sendPipeline.ExecuteAsync(scheduleContext, cancellationToken: cancellationToken);
    }
}
