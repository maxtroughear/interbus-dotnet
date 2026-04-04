using InterBus.Middleware;
using TickerQ.Utilities.Base;

namespace InterBus.TickerQ.Internal;

/// <summary>
/// TickerQ function handler for all scheduled InterBus messages (both topic fan-out and
/// point-to-point queue send). Resolved from DI via constructor injection; called by TickerQ
/// when the scheduled job fires.
/// </summary>
internal sealed class TickerQDeliverFunction(SchedulePipelineExecutor schedulePipeline)
{
    /// <summary>
    /// TickerQ function name registered for all scheduled InterBus message delivery.
    /// </summary>
    public const string FunctionName = "interbus-deliver";

    /// <summary>
    /// Called by TickerQ when a scheduled delivery job fires. The typed context carries
    /// the <see cref="ScheduledDeliveryRequest"/> deserialized from the persisted request bytes.
    /// The message is passed through the <see cref="SchedulePipelineExecutor"/> so that any
    /// registered <see cref="IScheduleMiddleware"/> steps run before the message is forwarded
    /// to the correct <see cref="IInterBusTransport"/> via <see cref="IInterBusTransportFactory"/>.
    /// </summary>
    public Task InvokeAsync(TickerFunctionContext<ScheduledDeliveryRequest> context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var destination = new Uri(request.Destination);
        var headers = new MessageHeaders(request.Headers);
        var scheduleContext = new ScheduleContext(destination, request.Payload, headers, delay: null);
        return schedulePipeline.ExecuteAsync(scheduleContext, cancellationToken);
    }
}
