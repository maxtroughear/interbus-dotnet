using InterBus.Send;
using TickerQ.Utilities.Base;

namespace InterBus.TickerQ.Internal;

/// <summary>
/// TickerQ function handler for all scheduled InterBus messages (both topic fan-out and
/// point-to-point queue send). Resolved from DI via constructor injection; called by TickerQ
/// when the scheduled job fires.
/// </summary>
internal sealed class TickerQDeliverFunction(SendPipelineExecutor sendPipeline)
{
    /// <summary>
    /// TickerQ function name registered for all scheduled InterBus message deliveries
    /// </summary>
    public const string FunctionName = "interbus-deliver";

    public Task InvokeAsync(TickerFunctionContext<ScheduledDeliveryRequest> context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var destination = new Uri(request.Destination);
        var headers = new MessageHeaders(request.Headers);
        var scheduleContext = new SendContext(destination, request.Payload, headers, delay: null);
        return sendPipeline.ExecuteAsync(scheduleContext, cancellationToken: cancellationToken);
    }
}
